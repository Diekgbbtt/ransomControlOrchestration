import pandas as pd

import sys

import subprocess

import orchestration


def main():
    df = pd.read_csv("migration_details.csv")

    print(df.to_string())

    i = 0

    while(i <= df.shape[0]):
        args = []
        args.append(df.iat[0, i])
        args.append(df.iat[1, i])
        args.append(df.iat[2, i])

        process = subprocess.run(["python", "dpx_integration.py", args[0], args[1], args[2]], capture_output=True)
        
        i = i + 1

        while 1:
            output = process.stdout.decode("utf-8")
            error = process.stderr.decode("utf-8")
            if output:
                print(output)
            if error:
                print(error)
            if output == "" and error ==  "":
                print(f"process completed with return code: " + process.returncode)
                break

    # dpx_integration.main(args[0], args[1], args[2])

if __name__ == "__main__":
    main()



