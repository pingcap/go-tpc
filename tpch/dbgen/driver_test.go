package dbgen

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

var gotOrdersBuf bytes.Buffer
var expectOrders = `1|36901|O|173665.47|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |
2|78002|O|46929.18|1996-12-01|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot|
3|123314|F|193846.25|1993-10-14|5-LOW|Clerk#000000955|0|sly final accounts boost. carefully regular ideas cajole carefully. depos|
4|136777|O|32151.78|1995-10-11|5-LOW|Clerk#000000124|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro|
5|44485|F|144659.20|1994-07-30|5-LOW|Clerk#000000925|0|quickly. bold deposits sleep slyly. packages use slyly|
6|55624|F|58749.59|1992-02-21|4-NOT SPECIFIED|Clerk#000000058|0|ggle. special, final requests are against the furiously specia|
7|39136|O|252004.18|1996-01-10|2-HIGH|Clerk#000000470|0|ly special requests |
32|130057|O|208660.75|1995-07-16|2-HIGH|Clerk#000000616|0|ise blithely bold, regular requests. quickly unusual dep|
33|66958|F|163243.98|1993-10-27|3-MEDIUM|Clerk#000000409|0|uriously. furiously final request|
34|61001|O|58949.67|1998-07-21|3-MEDIUM|Clerk#000000223|0|ly final packages. fluffily final deposits wake blithely ideas. spe|
`

var gotLinesBuf bytes.Buffer
var expectLines = `1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|
1|67310|7311|2|36|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |
1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|
1|2132|4633|4|28|28955.64|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-16|NONE|AIR|lites. fluffily even de|
1|24027|1534|5|24|22824.48|0.10|0.04|N|O|1996-03-30|1996-03-14|1996-04-01|NONE|FOB| pending foxes. slyly re|
1|15635|638|6|32|49620.16|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-02-03|DELIVER IN PERSON|MAIL|arefully slyly ex|
2|106170|1191|1|38|44694.46|0.00|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests. deposits breach a|
3|4297|1798|1|45|54058.05|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|
3|19036|6540|2|49|46796.47|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|
3|128449|3474|3|27|39890.88|0.06|0.07|A|F|1994-01-16|1993-11-22|1994-01-23|DELIVER IN PERSON|SHIP|nal foxes wake. |
3|29380|1883|4|2|2618.76|0.01|0.06|A|F|1993-12-04|1994-01-07|1994-01-01|NONE|TRUCK|y. fluffily pending d|
3|183095|650|5|28|32986.52|0.04|0.00|R|F|1993-12-14|1994-01-10|1994-01-01|TAKE BACK RETURN|FOB|ages nag slyly pending|
3|62143|9662|6|26|28733.64|0.10|0.02|A|F|1993-10-29|1993-12-18|1993-11-04|TAKE BACK RETURN|RAIL|ges sleep after the caref|
4|88035|5560|1|30|30690.90|0.03|0.08|N|O|1996-01-10|1995-12-14|1996-01-18|DELIVER IN PERSON|REG AIR|- quickly regular packages sleep. idly|
5|108570|8571|1|15|23678.55|0.02|0.04|R|F|1994-10-31|1994-08-31|1994-11-20|NONE|AIR|ts wake furiously |
5|123927|3928|2|26|50723.92|0.07|0.08|R|F|1994-10-16|1994-09-25|1994-10-19|NONE|FOB|sts use slyly quickly special instruc|
5|37531|35|3|50|73426.50|0.08|0.03|A|F|1994-08-08|1994-10-13|1994-08-26|DELIVER IN PERSON|AIR|eodolites. fluffily unusual|
6|139636|2150|1|37|61998.31|0.08|0.03|A|F|1992-04-27|1992-05-15|1992-05-02|TAKE BACK RETURN|TRUCK|p furiously special foxes|
7|182052|9607|1|12|13608.60|0.07|0.03|N|O|1996-05-07|1996-03-13|1996-06-03|TAKE BACK RETURN|FOB|ss pinto beans wake against th|
7|145243|7758|2|9|11594.16|0.08|0.08|N|O|1996-02-01|1996-03-02|1996-02-19|TAKE BACK RETURN|SHIP|es. instructions|
7|94780|9799|3|46|81639.88|0.10|0.07|N|O|1996-01-15|1996-03-27|1996-02-03|COLLECT COD|MAIL| unusual reques|
7|163073|3074|4|28|31809.96|0.03|0.04|N|O|1996-03-21|1996-04-08|1996-04-20|NONE|FOB|. slyly special requests haggl|
7|151894|9440|5|38|73943.82|0.08|0.01|N|O|1996-02-11|1996-02-24|1996-02-18|DELIVER IN PERSON|TRUCK|ns haggle carefully ironic deposits. bl|
7|79251|1759|6|35|43058.75|0.06|0.03|N|O|1996-01-16|1996-02-23|1996-01-22|TAKE BACK RETURN|FOB|jole. excuses wake carefully alongside of |
7|157238|2269|7|5|6476.15|0.04|0.02|N|O|1996-02-10|1996-03-26|1996-02-13|NONE|FOB|ithely regula|
32|82704|7721|1|28|47227.60|0.05|0.08|N|O|1995-10-23|1995-08-27|1995-10-26|TAKE BACK RETURN|TRUCK|sleep quickly. req|
32|197921|441|2|32|64605.44|0.02|0.00|N|O|1995-08-14|1995-10-07|1995-08-27|COLLECT COD|AIR|lithely regular deposits. fluffily |
32|44161|6666|3|2|2210.32|0.09|0.02|N|O|1995-08-07|1995-10-07|1995-08-23|DELIVER IN PERSON|AIR| express accounts wake according to the|
32|2743|7744|4|4|6582.96|0.09|0.03|N|O|1995-08-04|1995-10-01|1995-09-03|NONE|REG AIR|e slyly final pac|
32|85811|8320|5|44|79059.64|0.05|0.06|N|O|1995-08-28|1995-08-20|1995-09-14|DELIVER IN PERSON|AIR|symptotes nag according to the ironic depo|
32|11615|4117|6|6|9159.66|0.04|0.03|N|O|1995-07-21|1995-09-23|1995-07-25|COLLECT COD|RAIL| gifts cajole carefully.|
33|61336|8855|1|31|40217.23|0.09|0.04|A|F|1993-10-29|1993-12-19|1993-11-08|COLLECT COD|TRUCK|ng to the furiously ironic package|
33|60519|5532|2|32|47344.32|0.02|0.05|A|F|1993-12-09|1994-01-04|1993-12-28|COLLECT COD|MAIL|gular theodolites|
33|137469|9983|3|5|7532.30|0.05|0.03|A|F|1993-12-09|1993-12-25|1993-12-23|TAKE BACK RETURN|AIR|. stealthily bold exc|
33|33918|3919|4|41|75928.31|0.09|0.00|R|F|1993-11-09|1994-01-24|1993-11-11|TAKE BACK RETURN|MAIL|unusual packages doubt caref|
34|88362|871|1|13|17554.68|0.00|0.07|N|O|1998-10-23|1998-09-14|1998-11-06|NONE|REG AIR|nic accounts. deposits are alon|
34|89414|1923|2|22|30875.02|0.08|0.06|N|O|1998-10-09|1998-10-16|1998-10-12|NONE|FOB|thely slyly p|
34|169544|4577|3|6|9681.24|0.02|0.06|N|O|1998-10-30|1998-09-20|1998-11-05|NONE|FOB|ar foxes sleep |
`

var gotSuppsBuf bytes.Buffer
var expectSupps = `1|Supplier#000000001| N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ|17|27-918-335-1736|5755.94|each slyly above the careful|
2|Supplier#000000002|89eJ5ksX3ImxJQBvxObC,|5|15-679-861-2259|4032.68| slyly bold instructions. idle dependen|
3|Supplier#000000003|q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3|1|11-383-516-1199|4192.40|blithely silent requests after the express dependencies are sl|
4|Supplier#000000004|Bk7ah4CK8SYQTepEmvMkkgMwg|15|25-843-787-7479|4641.08|riously even requests above the exp|
5|Supplier#000000005|Gcdm2rJRzl5qlTVzc|11|21-151-690-3663|-283.84|. slyly regular pinto bea|
6|Supplier#000000006|tQxuVm7s7CnK|14|24-696-997-4969|1365.79|final accounts. regular dolphins use against the furiously ironic decoys. |
7|Supplier#000000007|s,4TicNGB4uO6PaSqNBUq|23|33-990-965-2201|6820.35|s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit|
8|Supplier#000000008|9Sq4bBH2FQEmaFOocY45sRTxo6yuoG|17|27-498-742-3860|7627.85|al pinto beans. asymptotes haggl|
9|Supplier#000000009|1KhUgZegwM3ua7dsYmekYBsK|10|20-403-398-8662|5302.37|s. unusual, even requests along the furiously regular pac|
10|Supplier#000000010|Saygah3gYWMp72i PY|24|34-852-489-8585|3891.91|ing waters. regular requests ar|
`

var gotCustsBuf bytes.Buffer
var expectCusts = `1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e|
2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: caref|
3|Customer#000000003|MG9kdTD2WBHm|1|11-719-748-3364|7498.12|AUTOMOBILE| deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov|
4|Customer#000000004|XxVSJsLAGtn|4|14-128-190-5944|2866.83|MACHINERY| requests. final, regular ideas sleep final accou|
5|Customer#000000005|KvpyuHCplrB84WgAiGV6sYpZq7Tj|3|13-750-942-6364|794.47|HOUSEHOLD|n accounts will have to unwind. foxes cajole accor|
6|Customer#000000006|sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn|20|30-114-968-4951|7638.57|AUTOMOBILE|tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious|
7|Customer#000000007|TcGe5gaZNgVePxU5kRrvXBfkasDTea|18|28-190-982-9759|9561.95|AUTOMOBILE|ainst the ironic, express theodolites. express, even pinto beans among the exp|
8|Customer#000000008|I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5|17|27-147-574-9335|6819.74|BUILDING|among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide|
9|Customer#000000009|xKiAFTjUsCuxfeleNqefumTrjS|8|18-338-906-3675|8324.07|FURNITURE|r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl|
10|Customer#000000010|6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2|5|15-741-346-9870|2753.54|HOUSEHOLD|es regular deposits haggle. fur|
`

var gotPartsBuf bytes.Buffer
var expectParts = `1|goldenrod lavender spring chocolate lace|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|7|JUMBO PKG|901.00|ly. slyly ironi|
2|blush thistle blue yellow saddle|Manufacturer#1|Brand#13|LARGE BRUSHED BRASS|1|LG CASE|902.00|lar accounts amo|
3|spring green yellow purple cornsilk|Manufacturer#4|Brand#42|STANDARD POLISHED BRASS|21|WRAP CASE|903.00|egular deposits hag|
4|cornflower chocolate smoke green pink|Manufacturer#3|Brand#34|SMALL PLATED BRASS|14|MED DRUM|904.00|p furiously r|
5|forest brown coral puff cream|Manufacturer#3|Brand#32|STANDARD POLISHED TIN|15|SM PKG|905.00| wake carefully |
6|bisque cornflower lawn forest magenta|Manufacturer#2|Brand#24|PROMO PLATED STEEL|4|MED BAG|906.00|sual a|
7|moccasin green thistle khaki floral|Manufacturer#1|Brand#11|SMALL PLATED COPPER|45|SM BAG|907.00|lyly. ex|
8|misty lace thistle snow royal|Manufacturer#4|Brand#44|PROMO BURNISHED TIN|41|LG DRUM|908.00|eposi|
9|thistle dim navajo dark gainsboro|Manufacturer#4|Brand#43|SMALL BURNISHED STEEL|12|WRAP CASE|909.00|ironic foxe|
10|linen pink saddle puff powder|Manufacturer#5|Brand#54|LARGE BURNISHED STEEL|44|LG CAN|910.01|ithely final deposit|
`

var gotPsuppsBuf bytes.Buffer
var expectPsupps = `1|2|3325|771.64|, even theodolites. regular, final theodolites eat after the carefully pending foxes. furiously regular deposits sleep slyly. carefully bold realms above the ironic dependencies haggle careful|
1|2502|8076|993.49|ven ideas. quickly even packages print. pending multipliers must have to are fluff|
1|5002|3956|337.09|after the fluffily ironic deposits? blithely special dependencies integrate furiously even excuses. blithely silent theodolites could have to haggle pending, express requests; fu|
1|7502|4069|357.84|al, regular dependencies serve carefully after the quickly final pinto beans. furiously even deposits sleep quickly final, silent pinto beans. fluffily reg|
2|3|8895|378.49|nic accounts. final accounts sleep furiously about the ironic, bold packages. regular, regular accounts|
2|2503|4969|915.27|ptotes. quickly pending dependencies integrate furiously. fluffily ironic ideas impress blithely above the express accounts. furiously even epitaphs need to wak|
2|5003|8539|438.37|blithely bold ideas. furiously stealthy packages sleep fluffily. slyly special deposits snooze furiously carefully regular accounts. regular deposits according to the accounts nag carefully slyl|
2|7503|3025|306.39|olites. deposits wake carefully. even, express requests cajole. carefully regular ex|
3|4|4651|920.92|ilent foxes affix furiously quickly unusual requests. even packages across the carefully even theodolites nag above the sp|
3|2504|4093|498.13|ending dependencies haggle fluffily. regular deposits boost quickly carefully regular requests. deposits affix furiously around the pinto beans. ironic, unusual platelets across the p|
3|5004|3917|645.40|of the blithely regular theodolites. final theodolites haggle blithely carefully unusual ideas. blithely even f|
3|7504|9942|191.92| unusual, ironic foxes according to the ideas detect furiously alongside of the even, express requests. blithely regular the|
4|5|1339|113.97| carefully unusual ideas. packages use slyly. blithely final pinto beans cajole along the furiously express requests. regular orbits haggle carefully. care|
4|2505|6377|591.18|ly final courts haggle carefully regular accounts. carefully regular accounts could integrate slyly. slyly express packages about the accounts wake slyly|
4|5005|2694|51.37|g, regular deposits: quick instructions run across the carefully ironic theodolites-- final dependencies haggle into the dependencies. f|
4|7505|2480|444.37|requests sleep quickly regular accounts. theodolites detect. carefully final depths w|
5|6|3735|255.88|arefully even requests. ironic requests cajole carefully even dolphin|
5|2506|9653|50.52|y stealthy deposits. furiously final pinto beans wake furiou|
5|5006|1329|219.83|iously regular deposits wake deposits. pending pinto beans promise ironic dependencies. even, regular pinto beans integrate|
5|7506|6925|537.98|sits. quickly fluffy packages wake quickly beyond the blithely regular requests. pending requests cajole among the final pinto beans. carefully busy theodolites affix quickly stealthily |
6|7|8851|130.72|usly final packages. slyly ironic accounts poach across the even, sly requests. carefully pending request|
6|2507|1627|424.25| quick packages. ironic deposits print. furiously silent platelets across the carefully final requests are slyly along the furiously even instructi|
6|5007|3336|642.13|final instructions. courts wake packages. blithely unusual realms along the multipliers nag |
6|7507|6451|175.32| accounts alongside of the slyly even accounts wake carefully final instructions-- ruthless platelets wake carefully ideas. even deposits are quickly final,|
7|8|7454|763.98|y express tithes haggle furiously even foxes. furiously ironic deposits sleep toward the furiously unusual|
7|2508|2770|149.66|hould have to nag after the blithely final asymptotes. fluffily spe|
7|5008|3377|68.77|usly against the daring asymptotes. slyly regular platelets sleep quickly blithely regular deposits. boldly regular deposits wake blithely ironic accounts|
7|7508|9460|299.58|. furiously final ideas hinder slyly among the ironic, final packages. blithely ironic dependencies cajole pending requests: blithely even packa|
8|9|6834|249.63|lly ironic accounts solve express, unusual theodolites. special packages use quickly. quickly fin|
8|2509|396|957.34|r accounts. furiously pending dolphins use even, regular platelets. final|
8|5009|9845|220.62|s against the fluffily special packages snooze slyly slyly regular p|
8|7509|8126|916.91|final accounts around the blithely special asymptotes wake carefully beyond the bold dugouts. regular ideas haggle furiously after|
9|10|7054|84.20|ts boost. evenly regular packages haggle after the quickly careful accounts. |
9|2510|7542|811.84|ate after the final pinto beans. express requests cajole express packages. carefully bold ideas haggle furiously. blithely express accounts eat carefully among the evenly busy accounts. carefully un|
9|5010|9583|381.31|d foxes. final, even braids sleep slyly slyly regular ideas. unusual ideas above|
9|7510|3063|291.84| the blithely ironic instructions. blithely express theodolites nag furiously. carefully bold requests shall have to use slyly pending requests. carefully regular instr|
10|11|2952|996.12| bold foxes wake quickly even, final asymptotes. blithely even depe|
10|2511|3335|673.27|s theodolites haggle according to the fluffily unusual instructions. silent realms nag carefully ironic theodolites. furiously unusual instructions would detect fu|
10|5011|5691|164.00|r, silent instructions sleep slyly regular pinto beans. furiously unusual gifts use. silently ironic theodolites cajole final deposits! express dugouts are furiously. packages sleep |
10|7511|841|374.02|refully above the ironic packages. quickly regular packages haggle foxes. blithely ironic deposits a|
`

var gotNationsBuf bytes.Buffer
var expectNations = `0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special |
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold|
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d|
5|ETHIOPIA|0|ven packages wake quickly. regu|
6|FRANCE|3|refully final requests. regular, ironi|
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco|
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun|
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull|
10|IRAN|4|efully alongside of the slyly final dependencies. |
11|IRAQ|4|nic deposits boost atop the quickly final requests? quickly regula|
12|JAPAN|2|ously. final, express gifts cajole a|
13|JORDAN|4|ic deposits are blithely about the carefully regular pa|
14|KENYA|0| pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t|
15|MOROCCO|0|rns. blithely bold courts among the closely regular packages use furiously bold platelets?|
16|MOZAMBIQUE|0|s. ironic, unusual asymptotes wake blithely r|
17|PERU|1|platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun|
18|CHINA|2|c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos|
19|ROMANIA|3|ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account|
20|SAUDI ARABIA|4|ts. silent requests haggle. closely express packages sleep across the blithely|
21|VIETNAM|2|hely enticingly express accounts. even, final |
22|RUSSIA|3| requests against the platelets use never according to the quickly regular pint|
23|UNITED KINGDOM|3|eans boost carefully special requests. accounts are. carefull|
24|UNITED STATES|1|y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be|
`

var gotRegionsBuf bytes.Buffer
var expectRegions = `0|AFRICA|lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to |
1|AMERICA|hs use ironic, even requests. s|
2|ASIA|ges. thinly even pinto beans ca|
3|EUROPE|ly final courts cajole furiously final excuse|
4|MIDDLE EAST|uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl|
`

func TestMain(m *testing.M) {
	initDriver(1)

	testOrderLoader := func(order interface{}) error {
		o := order.(*Order)
		gotOrdersBuf.WriteString(fmt.Sprintf("%d|%d|%c|%s|%s|%s|%s|%d|%s|\n",
			o.oKey,
			o.custKey,
			o.status,
			fmtMoney(o.totalPrice),
			o.date,
			o.orderPriority,
			o.clerk,
			o.shipPriority,
			o.comment))

		return nil
	}

	testLineLoader := func(order interface{}) error {
		o := order.(*Order)
		for _, line := range o.lines {
			if _, err := gotLinesBuf.WriteString(
				fmt.Sprintf("%d|%d|%d|%d|%d|%s|%s|%s|%c|%c|%s|%s|%s|%s|%s|%s|\n",
					line.oKey,
					line.partKey,
					line.suppKey,
					line.lCnt,
					line.quantity,
					fmtMoney(line.ePrice),
					fmtMoney(line.discount),
					fmtMoney(line.tax),
					line.rFlag,
					line.lStatus,
					line.sDate,
					line.cDate,
					line.rDate,
					line.shipInstruct,
					line.shipMode,
					line.comment,
				)); err != nil {
				return err
			}
		}

		return nil
	}

	testSuppLoader := func(supp interface{}) error {
		s := supp.(*Supp)
		if _, err := gotSuppsBuf.WriteString(
			fmt.Sprintf("%d|%s|%s|%d|%s|%s|%s|\n",
				s.suppKey,
				s.name,
				s.address,
				s.nationCode,
				s.phone,
				fmtMoney(s.acctbal),
				s.comment)); err != nil {
			return err
		}
		return nil
	}

	testCustLoader := func(cust interface{}) error {
		c := cust.(*Cust)
		if _, err := gotCustsBuf.WriteString(
			fmt.Sprintf("%d|%s|%s|%d|%s|%s|%s|%s|\n",
				c.custKey,
				c.name,
				c.address,
				c.nationCode,
				c.phone,
				fmtMoney(c.acctbal),
				c.mktSegment,
				c.comment)); err != nil {
			return err
		}
		return nil
	}

	testPartLoader := func(part interface{}) error {
		p := part.(*Part)
		if _, err := gotPartsBuf.WriteString(fmt.Sprintf("%d|%s|%s|%s|%s|%d|%s|%s|%s|\n",
			p.partKey,
			p.name,
			p.mfgr,
			p.brand,
			p.types,
			p.size,
			p.container,
			fmtMoney(p.retailPrice),
			p.comment)); err != nil {
			return err
		}
		return nil
	}

	testPSuppLoader := func(part interface{}) error {
		p := part.(*Part)
		for i := 0; i < SUPP_PER_PART; i++ {
			supp := p.s[i]
			if _, err := gotPsuppsBuf.WriteString(
				fmt.Sprintf("%d|%d|%d|%s|%s|\n",
					supp.partKey,
					supp.suppKey,
					supp.qty,
					fmtMoney(supp.sCost),
					supp.comment)); err != nil {
				return err
			}
		}
		return nil
	}

	var testNationLoader = func(nation interface{}) error {
		n := nation.(*Nation)
		if _, err := gotNationsBuf.WriteString(
			fmt.Sprintf("%d|%s|%d|%s|\n",
				n.code,
				n.text,
				n.join,
				n.comment)); err != nil {
			return err
		}
		return nil
	}

	var testRegionLoader = func(region interface{}) error {
		r := region.(*Region)
		if _, err := gotRegionsBuf.WriteString(
			fmt.Sprintf("%d|%s|%s|\n",
				r.code,
				r.text,
				r.comment)); err != nil {
			return err
		}
		return nil
	}

	*orderLoader = testOrderLoader
	*lineItemLoader = testLineLoader
	*suppLoader = testSuppLoader
	*custLoader = testCustLoader
	*partLoader = testPartLoader
	*partSuppLoader = testPSuppLoader
	*nationLoader = testNationLoader
	*regionLoader = testRegionLoader

	os.Exit(m.Run())
}

func TestGenOrder(t *testing.T) {
	if err := genTbl(ORDER, 1, 10); err != nil {
		t.Error(err)
	}

	gotOrders := gotOrdersBuf.String()
	if gotOrders != expectOrders {
		t.Errorf("expect:\n%s\ngot:\n%s", expectOrders, gotOrders)
	}
}

func TestGenLine(t *testing.T) {
	if err := genTbl(LINE, 1, 10); err != nil {
		t.Error(err)
	}

	gotLines := gotLinesBuf.String()
	if gotLines != expectLines {
		t.Errorf("expect:\n%s\ngot:\n%s", expectLines, gotLines)
	}
}

func TestGenOrderLine(t *testing.T) {
	if err := genTbl(ORDER_LINE, 1, 10); err != nil {
		t.Error(err)
	}
	gotOrders := gotOrdersBuf.String()
	if gotOrders != expectOrders {
		t.Errorf("expect:\n%s\ngot:\n%s", expectOrders, gotOrders)
	}
	gotLines := gotLinesBuf.String()
	if gotLines != expectLines {
		t.Errorf("expect:\n%s\ngot:\n%s", expectLines, gotLines)
	}
}

func TestGenSupp(t *testing.T) {
	genTbl(SUPP, 1, 10)

	gotSupp := gotSuppsBuf.String()
	if gotSupp != expectSupps {
		t.Errorf("expect:\n%s\ngot:\n%s", expectSupps, gotSupp)
	}
}

func TestGenCust(t *testing.T) {
	genTbl(CUST, 1, 10)

	gotCusts := gotCustsBuf.String()
	if gotCusts != expectCusts {
		t.Errorf("expect:\n%s\ngot:\n%s", gotCusts, gotCusts)
	}
}

func TestGenPart(t *testing.T) {
	genTbl(PART, 1, 10)

	gotParts := gotPartsBuf.String()
	if gotParts != expectParts {
		t.Errorf("expect:\n%s\ngot:\n%s", expectParts, gotParts)
	}
}

func TestGenPartSupp(t *testing.T) {
	genTbl(PSUPP, 1, 10)

	gotPsupps := gotPsuppsBuf.String()
	if gotPsupps != expectPsupps {
		t.Errorf("expect:\n%s\ngot:\n%s", expectPsupps, gotPsupps)
	}
}

func TestGenPartPsupp(t *testing.T) {
	genTbl(PART_PSUPP, 1, 10)

	gotParts := gotPartsBuf.String()
	if gotParts != expectParts {
		t.Errorf("expect:\n%s\ngot:\n%s", expectParts, gotParts)
	}

	gotPsupps := gotPsuppsBuf.String()
	if gotPsupps != expectPsupps {
		t.Errorf("expect:\n%s\ngot:\n%s", expectPsupps, gotPsupps)
	}
}

func TestGenNation(t *testing.T) {
	genTbl(NATION, 1, 25)

	gotNations := gotNationsBuf.String()
	if gotNations != expectNations {
		t.Errorf("expect:\n%s\ngot:\n%s", expectNations, gotNations)
	}
}

func TestGenRegion(t *testing.T) {
	genTbl(REGION, 1, 5)

	gotRegions := gotRegionsBuf.String()
	if gotRegions != expectRegions {
		t.Errorf("expect:\n%s\ngot:\n%s", expectRegions, gotRegions)
	}
}
