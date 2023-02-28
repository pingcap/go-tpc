package plan_replayer

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type PlanReplayerConfig struct {
	Host                 string
	StatusPort           int
	WorkloadName         string
	PlanReplayerDir      string
	PlanReplayerFileName string
}

type PlanReplayerRunner struct {
	sync.Mutex
	prepared bool
	finished bool
	Config   PlanReplayerConfig
	zf       *os.File
	zw       struct {
		writer *zip.Writer
	}
}

func (r *PlanReplayerRunner) Prepare() error {
	r.Lock()
	defer r.Unlock()
	if r.prepared {
		return nil
	}
	if r.Config.PlanReplayerDir == "" {
		dir, err := os.Getwd()
		if err != nil {
			return err
		}
		r.Config.PlanReplayerDir = dir
	}
	if r.Config.PlanReplayerFileName == "" {
		r.Config.PlanReplayerFileName = fmt.Sprintf("plan_replayer_%s_%s",
			r.Config.WorkloadName, time.Now().Format("2006-01-02-15:04:05"))
	}

	fileName := fmt.Sprintf("%s.zip", r.Config.PlanReplayerFileName)
	zf, err := os.Create(filepath.Join(r.Config.PlanReplayerDir, fileName))
	if err != nil {
		return err
	}
	r.zf = zf
	// Create zip writer
	r.zw.writer = zip.NewWriter(zf)
	r.prepared = true
	return nil
}

func (r *PlanReplayerRunner) Finish() error {
	r.Lock()
	defer r.Unlock()
	if r.finished {
		return nil
	}
	err := r.zw.writer.Close()
	if err != nil {
		return err
	}
	r.finished = true
	return r.zf.Close()
}

func (r *PlanReplayerRunner) Dump(ctx context.Context, conn *sql.Conn, query, queryName string) error {
	r.Lock()
	defer r.Unlock()
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute query %s failed %v", query, err)
	}
	defer rows.Close()
	var token string
	for rows.Next() {
		err := rows.Scan(&token)
		if err != nil {
			return fmt.Errorf("execute query %s failed %v", query, err)
		}
	}
	// TODO: support tls
	resp, err := http.Get(fmt.Sprintf("http://%s:%v/plan_replayer/dump/%s", r.Config.Host, r.Config.StatusPort, token))
	if err != nil {
		return fmt.Errorf("get plan replayer for query %s failed %v", queryName, err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("get plan replayer for query %s failed %v", queryName, err)
	}
	err = r.writeDataIntoZW(b, queryName)
	if err != nil {
		return fmt.Errorf("dump plan replayer for %s failed %v", queryName, err)
	}
	return nil
}

// writeDataIntoZW will dump query stats information by following format in zip
/*
 |-q1_time.zip
 |-q2_time.zip
 |-q3_time.zip
 |-...
*/
func (r *PlanReplayerRunner) writeDataIntoZW(b []byte, queryName string) error {
	k := make([]byte, 16)
	//nolint: gosec
	_, err := rand.Read(k)
	if err != nil {
		return err
	}
	key := base64.URLEncoding.EncodeToString(k)
	wr, err := r.zw.writer.Create(fmt.Sprintf("%v_%v_%v.zip",
		queryName, time.Now().Format("2006-01-02-15:04:05"), key))
	if err != nil {
		return err
	}
	_, err = wr.Write(b)
	if err != nil {
		return err
	}
	return nil
}
