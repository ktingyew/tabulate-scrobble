from datetime import datetime
import glob
import pathlib
import re

def find_file_with_latest_dt_in_dir(
    directory: pathlib.Path,
    re_search: str = r"\b20.*00",
    dt_formatter: str = "%Y-%m-%d %H-%M-%S",

) -> pathlib.Path :
    """
    Lol
    """

    dt_ls = []
    for f in glob.glob(str(directory/'*')):
        dt_ls.append(
            datetime.strptime(
                re.search(re_search, f).group(),
                dt_formatter
            )
        )

    dt_latest = max(dt_ls).strftime("%Y-%m-%d %H-%M-%S")

    return pathlib.Path(
        glob.glob(str(directory/f'*{dt_latest}*'))[0]
    )