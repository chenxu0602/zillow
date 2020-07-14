import os, sys
import pandas as pd
import quandl
from pathlib import Path
import concurrent.futures

output_dir = Path("data")
if not output_dir.exists():
    print(f"Output dir {output_dir} doesn't exist, creating one ...")
    os.makedirs(output_dir)

try:
    df_meta = pd.read_csv("ZILLOW_metadata.csv")
#    df_meta = df_meta.iloc[:100]
except Exception as e:
    print(f"Couldn't get meta data!")
    sys.exit(1)
else:
    print(f"{len(df_meta)} names in total.")


quandl.ApiConfig.api_key = 'd5KcMbVrv2GRC2H9Qrn4'

def download(code, start, output):
    df = quandl.get(f"ZILLOW/{code}", start_date=start)
    df.to_csv(output, index=True)
    return df

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    future_to_url = {}
    for i in range(len(df_meta)):
        code, name, desc, refresh, from_date, to_date = df_meta.iloc[i]
        output = output_dir / f"{code}.csv"
        if output.exists(): 
            print(f"{output} exists, skip ...")
            continue
        future_to_url[executor.submit(download, code, from_date, output)] = code

    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
        except Exception as exc:
            print(f"{url} generated an exception: {exc}")
        else:
            print(f"{url} is {len(data)} lines.")
        

