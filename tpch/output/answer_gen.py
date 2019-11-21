if __name__ == "__main__":
    output = """package tpch

"""
    max_columns = 1

    for i in range(1, 23):
        output += f"""var q{i}a = [][]string{{"""
        with open(f"output/q{i}.out", "r") as infile:
            line = infile.readline()
            line = infile.readline().rstrip('\r\n')
            while line:
                columns = list(map(lambda column: f"`{column}`", line.split("|")))
                max_columns = max(max_columns, len(columns))

                l = ", ".join(columns)
                output = f"""{output}
    {{{l}}},"""
                line = infile.readline().rstrip('\r\n')
        output = f"""{output}}}
"""

    output += f"""
var ans = map[string][][]string{{
	"q1":  q1a,
	"q2":  q2a,
	"q3":  q3a,
	"q4":  q4a,
	"q5":  q5a,
	"q6":  q6a,
	"q7":  q7a,
	"q8":  q8a,
	"q9":  q9a,
	"q10": q10a,
	"q11": q11a,
	"q12": q12a,
	"q13": q13a,
	"q14": q14a,
	"q15": q15a,
	"q16": q16a,
	"q17": q17a,
	"q18": q18a,
	"q19": q19a,
	"q20": q20a,
	"q21": q21a,
	"q22": q22a,
}}

const maxColumns = {max_columns}
"""
    print(output)
