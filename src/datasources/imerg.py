import datetime
import os
import platform
import shutil
import tempfile
from pathlib import Path
from subprocess import Popen
from typing import Literal

import pandas as pd
import requests
import xarray as xr

from src.utils import blob

IMERG_BASE_URL = (
    "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGD"
    "{run}.0{version}/{date:%Y}/{date:%m}/3B-DAY-{run}.MS.MRG.3IMERG."
    "{date:%Y%m%d}-S000000-E235959.V0{version}{version_letter}.nc4"
)


def download_recent_imerg():
    """
    Downloads and processes all IMERG LATE v7 data from 2024-06-01 to yesterday
    Returns
    -------

    """
    existing_files = [
        x.name
        for x in blob.get_glb_container_client().list_blobs(
            name_starts_with="imerg/v7"
        )
    ]
    for date in pd.date_range(
        "2024-06-01", datetime.date.today() - pd.DateOffset(days=1)
    ):
        output_blob = (
            f"imerg/v7/imerg-daily-late-{date.strftime('%Y-%m-%d')}.tif"
        )
        if output_blob in existing_files:
            print(f"{output_blob} already exists, skipping")
            continue
        else:
            print(f"downloading and processing {output_blob}")
        try:
            download_imerg(date)
            da = process_imerg()
            upload_imerg(da, output_blob)
        except Exception as e:
            print(f"failed to download and process {date}")
            print(e)


def download_imerg(
    date: datetime.datetime,
    run: Literal["E", "L"] = "L",
    version: int = 7,
    save_path: str = Path("temp/imerg_temp.nc"),
    verbose: bool = False,
):
    """
    Downloads IMERG data for a given date and saves it to a temporary file
    Parameters
    ----------
    date: datetime.datetime
        Date to download
    run:
        "E" for early run, "L" for late run
    version: int
        IMERG version (7 is technically 07B)
    save_path: str
        Temporary path to save the downloaded file
    verbose:
        Whether to print out the URL and save path

    Returns
    -------

    """
    if os.path.exists(save_path):
        os.remove(save_path)
    version_letter = "B" if version == 7 else ""
    url = IMERG_BASE_URL.format(
        run=run, date=date, version=version, version_letter=version_letter
    )
    if verbose:
        print("downloading from " + url)
    result = requests.get(url)
    result.raise_for_status()
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
    f = open(save_path, "wb")
    f.write(result.content)
    f.close()
    if verbose:
        print("contents of URL written to " + save_path)


def process_imerg(path: str = "temp/imerg_temp.nc"):
    ds = xr.open_dataset(path)
    ds = ds.transpose("lat", "lon", "time", "nv")
    var_name = (
        "precipitationCal" if "precipitationCal" in ds else "precipitation"
    )
    da = ds[var_name]
    if not ds["time"].dtype == "<M8[ns]":
        da["time"] = pd.to_datetime(
            [pd.Timestamp(t.strftime("%Y-%m-%d")) for t in da["time"].values]
        )
    da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
    return da


def upload_imerg(da: xr.DataArray, output_path: str):
    """
    Saves DataArray to a temporary file and uploads it to the blob storage
    Parameters
    ----------
    da: xr.DataArray
        DataArray to upload
    output_path: str
        Blob name to upload to

    Returns
    -------

    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".tif") as tmpfile:
        temp_filename = tmpfile.name
        da.rio.to_raster(temp_filename, driver="COG")
        with open(temp_filename, "rb") as f:
            blob.get_glb_container_client().get_blob_client(
                output_path
            ).upload_blob(f, overwrite=True)


def create_auth_files():
    """
    Creates the necessary files for authentication with NASA GES DISC.
    Taken from
    https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Generate%20Earthdata%20Prerequisite%20Files
    Returns
    -------

    """
    IMERG_USERNAME = os.environ["IMERG_USERNAME"]
    IMERG_PASSWORD = os.environ["IMERG_PASSWORD"]

    urs = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication

    homeDir = os.path.expanduser("~") + os.sep

    with open(homeDir + ".netrc", "w") as file:
        file.write(
            "machine {} login {} password {}".format(
                urs, IMERG_USERNAME, IMERG_PASSWORD
            )
        )
        file.close()
    with open(homeDir + ".urs_cookies", "w") as file:
        file.write("")
        file.close()
    with open(homeDir + ".dodsrc", "w") as file:
        file.write("HTTP.COOKIEJAR={}.urs_cookies\n".format(homeDir))
        file.write("HTTP.NETRC={}.netrc".format(homeDir))
        file.close()

    print("Saved .netrc, .urs_cookies, and .dodsrc to:", homeDir)

    # Set appropriate permissions for Linux/macOS
    if platform.system() != "Windows":
        Popen("chmod og-rw ~/.netrc", shell=True)
    else:
        # Copy dodsrc to working directory in Windows
        shutil.copy2(homeDir + ".dodsrc", os.getcwd())
        print("Copied .dodsrc to:", os.getcwd())
