import re

if __name__ == "__main__":
    records = dict()
    record = None

    with open(f"dists.dss", "r") as infile:
        while True:
            line = infile.readline().rstrip('\r\n')
            if not line:
                break
            if re.match(r"^count|[0-9]+$", line, re.IGNORECASE):
                continue
            if re.match(r"^begin \w+$", line, re.IGNORECASE):
                n = line.split(" ")[1]
                record = []
                records[n] = record
                continue
            if re.match(r"^end \w+$", line, re.IGNORECASE):
                record = None
                continue
            if record is None:
                continue
            record.append(line.split("|"))

    # print(records)

    output = """package dist

type Item struct {
	Text   string
	Weight int32
}

type Dist []Item

var Maps = make(map[string]Dist)

func init() {"""

    for key, value in records.items():
        output += f"""
	Maps["{key}"] = []Item{{"""

        for v in value:
            output += f"""
		{{
			"{v[0]}",
			{v[1]},
		}},"""
        output += """
	}"""

    output += """
}
"""

    print(output)
