package tpcc

import (
	"context"
	"fmt"
	"time"
)

type paymentData struct {
	wID     int
	dID     int
	cWID    int
	cDID    int
	hAmount float64

	wStreet1 string
	wStreet2 string
	wCity    string
	wState   string
	wZip     string
	wName    string

	dStreet1 string
	dStreet2 string
	dCity    string
	dState   string
	dZip     string
	dName    string

	cID        int
	cFirst     string
	cMiddle    string
	cLast      string
	cStreet1   string
	cStreet2   string
	cCity      string
	cState     string
	cZip       string
	cPhone     string
	cSince     string
	cCredit    string
	cCreditLim float64
	cDiscount  float64
	cBalance   float64
	cData      string
}

func (w *Workloader) runPayment(ctx context.Context, thread int) error {
	s := w.getState(ctx)

	d := paymentData{
		wID:     randInt(s.R, 1, w.cfg.Warehouses),
		dID:     randInt(s.R, 1, districtPerWarehouse),
		hAmount: float64(randInt(s.R, 100, 500000)) / float64(100.0),
	}

	// Refer 2.5.1.2, 60% by last name, 40% by customer ID
	if s.R.Intn(100) < 60 {
		d.cLast = randCLast(s.R, s.Buf)
	} else {
		d.cID = randCustomerID(s.R)
	}

	// Refer 2.5.1.2, 85% by local, 15% by remote
	if w.cfg.Warehouses == 1 || s.R.Intn(100) < 85 {
		d.cWID = d.wID
		d.cDID = d.dID
	} else {
		d.cWID = w.otherWarehouse(ctx, d.wID)
		d.dID = randInt(s.R, 1, districtPerWarehouse)
	}

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// UPDATE warehouse SET w_ytd = w_ytd + :h_amount WHERE w_id=:w_id
	query := "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?"
	if _, err := tx.ExecContext(ctx, query, d.hAmount, d.wID); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	// SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name INTO
	// 	:w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name FROM warehouse
	// 	WHERE w_id=:w_id;
	query = `SELECT w_street_1, w_street_2, w_city, w_state, w_zip, 
w_name FROM warehouse WHERE w_id = ?`
	if err := tx.QueryRowContext(ctx, query, d.wID).Scan(&d.wStreet1, &d.wStreet2,
		&d.wCity, &d.wState, &d.wZip, &d.wName); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	// UPDATE district SET d_ytd = d_ytd + :h_amount
	// 	WHERE d_w_id = :w_id AND d_id = :d_id;
	query = "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?"
	if _, err := tx.ExecContext(ctx, query, d.hAmount, d.wID, d.dID); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	// SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
	//  INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
	// 	FROM district WHERE d_w_id = :w_id AND d_id = :d_id;
	query = `SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM 
district WHERE d_w_id = ? AND d_id = ?`
	if err := tx.QueryRowContext(ctx, query, d.wID, d.dID).Scan(&d.dStreet1, &d.dStreet2,
		&d.dCity, &d.dState, &d.dZip, &d.dName); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	if d.cID == 0 {
		// by name
		// SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last;
		var nameCnt int
		query = `SELECT count(c_id) namecnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?`
		if err := tx.QueryRowContext(ctx, query, d.cWID, d.cDID, d.cLast).Scan(&nameCnt); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// DECLARE c_byname_p CURSOR FOR SELECT c_id FROM customer
		//  WHERE c_w_id = :c_w_id
		// 	AND c_d_id = :c_d_id
		// 	AND c_last = :c_last
		// 	ORDER BY c_first;
		// OPEN c_byname_p

		if nameCnt%2 == 1 {
			nameCnt = nameCnt + 1
		}

		query = `SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first`
		rows, err := tx.QueryContext(ctx, query, d.cWID, d.cDID, d.cLast)
		if err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		for i := 0; i < nameCnt/2 && rows.Next(); i++ {
			if err := rows.Scan(&d.cID); err != nil {
				break
			}
		}
		// TODO: need to read all rows here?
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}

	// SELECT c_first, c_middle, c_last, c_street_1,
	// 	c_street_2, c_city, c_state, c_zip, c_phone,
	// 	c_credit, c_credit_lim, c_discount, c_balance, c_since
	// 	INTO :c_first, :c_middle, :c_last, :c_street_1,
	// 	:c_street_2, :c_city, :c_state, :c_zip, :c_phone,
	// 	:c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since
	// 	FROM customer WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id
	// 	AND c_id = :c_id FOR UPDATE;
	query = `SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? 
AND c_id = ? FOR UPDATE`
	if err := tx.QueryRowContext(ctx, query, d.cWID, d.cDID, d.dID).Scan(&d.cFirst, &d.cMiddle, &d.cLast,
		&d.cStreet1, &d.cStreet2, &d.cCity, &d.cState, &d.cZip, &d.cPhone, &d.cCredit, &d.cCreditLim,
		&d.cDiscount, &d.cBalance, &d.cSince); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	if d.cCredit == "BC" {
		// SELECT c_data INTO :c_data FROM customer
		// WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id
		query = `SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
		if err := tx.QueryRowContext(ctx, query, d.cWID, d.cDID, d.cID).Scan(&d.cData); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		newData := fmt.Sprintf("| %4d %2d %4d %2d %4d $%7.2f %12s %24s", d.cID, d.cDID, d.cWID,
			d.dID, d.wID, d.hAmount, time.Now().Format(timeFormat), d.cData)
		if len(newData) >= 500 {
			newData = newData[0:500]
		} else {
			newData += d.cData[0 : 500-len(newData)]
		}

		// UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
		// 	WHERE c_w_id = :c_w_id
		// 	AND c_d_id = :c_d_id AND c_id = :c_id;
		// refer 2.5.2.2 Case 2
		query = `UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, 
c_payment_cnt = c_payment_cnt + 1, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
		if _, err := tx.ExecContext(ctx, query, d.hAmount, d.hAmount, newData, d.cWID, d.cDID, d.cID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}
	} else {
		// UPDATE customer SET c_balance = :c_balance WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND
		//  c_id = :c_id;
		// refer 2.5.2.2 Case 1
		query = `UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, 
c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
		if _, err := tx.ExecContext(ctx, query, d.hAmount, d.hAmount, d.cWID, d.cDID, d.cID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}
	}

	// INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	// 	VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	hData := fmt.Sprintf("%10s    %10s", d.wName, d.dName)
	query = `INSERT INTO history (row_id, h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
VALUES (unhex(replace(uuid(), '-', '')), ?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, query, d.cDID, d.cWID, d.cID, d.dID, d.wID, time.Now().Format(timeFormat), d.hAmount, hData); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	return tx.Commit()
}
