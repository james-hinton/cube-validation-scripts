import os
import json
import boto3
import psycopg2
import psycopg2.extras

overview_path = "./overview.json"
known_files = ["datacube-metadata.yaml", "log_file.txt", "log_file.csv"]

try:
    with open(overview_path, "r") as f:
        overview = json.load(f)
except FileNotFoundError:
    print("Couldnt find overview JSON, please run create_overview() before trying to run any of these functions")

access_key = os.environ.get("S3_ACCESS_KEY_ID", None)
secret_key = os.environ.get("S3_SECRET_ACCESS_KEY", None)
region_name = os.environ.get("S3_REGION", "us-east-1")
endpoint_url = os.environ.get("S3_ENDPOINT", "https://s3-uk-1.sa-catapult.co.uk")
bucket_name = os.environ.get("S3_BUCKET", "public-eo-data")
s3_prefix = os.environ.get("S3_KEY", "common_sensing/fiji/")
stac_prefix = os.environ.get("S3_STAC_KEY", "stac_catalogs/cs_stac")

# Add to S3 bucket
s3_resource = boto3.resource(
    "s3",
    endpoint_url=endpoint_url,
    region_name=region_name,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)


# Creates overview JSON
def create_overview():
    """
    Creates a JSON with overview of whats in the s3_prefix, this must be done initially.
    """
    results = {}

    # Lists all platforms in the S3 'Directory'
    platforms = s3_resource.meta.client.list_objects_v2(
        Bucket=bucket_name, Prefix=s3_prefix, Delimiter="/"
    )

    # Loops through each platform
    for platform in platforms.get("CommonPrefixes", []):

        # Get the platform name
        sensor_name = platform["Prefix"].split("/")[-2]

        # Get the 'folders' in the platform
        scene_paginator = s3_resource.meta.client.get_paginator("list_objects_v2")
        scene_pages = scene_paginator.paginate(
            Bucket=bucket_name,
            Prefix=f"{s3_prefix}{sensor_name}/",
            Delimiter="/",
        )

        # Loop through the scenes of each platform
        platform_results = []
        for scene_page in scene_pages:
            if scene_page.get("CommonPrefixes"):
                for p in scene_page.get("CommonPrefixes"):
                    item_count = 0
                    prefix = p.get("Prefix")

                    # Loop through the items/files in each scene
                    item_paginator = s3_resource.meta.client.get_paginator(
                        "list_objects_v2"
                    )
                    item_pages = item_paginator.paginate(
                        Bucket=bucket_name, Prefix=prefix
                    )

                    # Increment the count
                    for item_page in item_pages:
                        if item_page.get("Contents"):
                            item_count += len(item_page.get("Contents"))

                    # Create an overview for the files in the scene
                    contents = item_page.get("Contents")
                    item_contents = []
                    for content in contents:
                        item_contents.append(
                            {
                                "Key": content.get("Key"),
                                "LastModified": content.get("LastModified").strftime(
                                    "%m/%d/%Y, %H:%M:%S"
                                ),
                                "Size": content.get("Size"),
                            }
                        )
                    item_info = {
                        "prefix": prefix,
                        "item_count": item_count,
                        "url": "https://public-eo-data.s3-uk-1.sa-catapult.co.uk/index.html?prefix="
                        + prefix,
                        "contents": item_contents,
                    }

                    # Add to the results
                    platform_results.append(item_info)

        # Sensor name as the key for the results
        results[sensor_name] = platform_results

    # Convert to JSON and write to file
    with open("./overview.json", "w") as outfile:
        json.dump(results, outfile)


# Check counts between the actual S3 and the S3 STACs
def check_s3_to_stac_counts():
    """
    Ensures that the STAC creation has picked up every item in the S3 Bucket
    """
    # Lists all platforms in the S3 'Directory'
    platforms = s3_resource.meta.client.list_objects_v2(
        Bucket=bucket_name, Prefix=stac_prefix, Delimiter="/"
    )
    results = {}

    for prefix in [stac_prefix, s3_prefix]:
        results[prefix] = {}
        for platform in platforms.get("CommonPrefixes", []):
            sensor_name = platform["Prefix"].split("/")[-2]
            count = 0
            paginator = s3_resource.meta.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket="public-eo-data",
                Prefix=f"{stac_prefix}{sensor_name}/",
                Delimiter="/",
            )

            for page in pages:
                for _ in page["CommonPrefixes"]:
                    count += 1

            results[prefix][sensor_name] = count

    for platform in results[stac_prefix]:
        if results[stac_prefix][platform] != results[s3_prefix][platform]:
            print(
                "Indifference in the count between STAC items and Tiffs for ", platform
            )


# Counts between the STACs and the cube database
def check_db_count():
    """
    Ensures that total number of items in the DB is the same as the items in the S3 Bucket
    """
    # Connect to DB
    connection = psycopg2.connect(
        host="localhost",
        database="datacube",
        user="postgres",
        password="postgres",
    )

    # create dictionary cursor
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get all platforms in the DB
    cursor.execute("SELECT * FROM agdc.dataset_type")
    dataset_type = cursor.fetchall()

    platforms = [i["name"] for i in dataset_type]

    for platform in platforms:
        # Get corrosponding ID from dataset_type
        for dataset in dataset_type:
            if dataset["name"].lower() == platform.lower():
                dataset_id = dataset["id"]
                break

        cursor.execute(
            "SELECT COUNT (*) FROM agdc.dataset WHERE dataset_type_ref = %s",
            (dataset_id,),
        )
        db_platform_count = cursor.fetchone()[0]

        if len(overview.get(platform, [])) != db_platform_count:
            print(
                f"Indifference in the count between S3 items and DB items for {platform}... {len(overview.get(platform, []))} vs {db_platform_count}"
            )


# Are counts of WOFS equal and which ones are missing
def check_wofs_count():
    """
    WOFs should be equal to the number of its base platform in the S3 bucket
    """
    platforms = s3_resource.meta.client.list_objects_v2(
        Bucket=bucket_name, Prefix=stac_prefix, Delimiter="/"
    )

    all_platforms = [
        platform["Prefix"].split("/")[-2]
        for platform in platforms.get("CommonPrefixes", [])
    ]

    for platform in all_platforms:
        if platform.endswith("_wofs"):
            base_platform = platform.replace("_wofs", "")

            if base_platform not in all_platforms:
                print("Can't find matching platform for", platform)

            results = {}

            for prefix in [stac_prefix, s3_prefix]:
                results[prefix] = {}
                for sensor in [platform, base_platform]:
                    count = 0
                    paginator = s3_resource.meta.client.get_paginator("list_objects_v2")
                    pages = paginator.paginate(
                        Bucket="public-eo-data",
                        Prefix=f"{prefix}{sensor}/",
                        Delimiter="/",
                    )

                    for page in pages:
                        count += len(page["CommonPrefixes"])

                    results[prefix][sensor] = count

            for prefix in [stac_prefix, s3_prefix]:
                if results[prefix][platform] != results[prefix][base_platform]:
                    print(
                        f"Indifference in the count between Wofs for {platform} and {base_platform} in {prefix}... {results[prefix][platform]} vs {results[prefix][base_platform]}",
                    )


# Find largest file in results
def find_largest_file():
    """
    Find largest file for each platform
    """
    largest_file = None
    largest_size = 0
    for platform in overview:
        for result in overview[platform]:
            for file in result["contents"]:
                if file["Size"] > largest_size:
                    largest_file = file
                    largest_size = file["Size"]
        print(platform, largest_file)


# Find smallest file in results
def find_smallest_file():
    """
    Smallest file for each platform
    """
    smallest_file = None
    smallest_size = float("inf")
    for platform in overview:
        for result in overview[platform]:
            for file in result["contents"]:
                if file["Size"] < smallest_size:
                    smallest_file = file
                    smallest_size = file["Size"]
        print(platform, smallest_file)


# Get the total size of all files in results
def get_total_size():
    """
    Total file size of all files in platform
    """
    total_size = 0
    for platform in overview:
        for result in overview[platform]:
            for file in result["contents"]:
                total_size += file["Size"]
        print(f"Total size of {platform} is {total_size}")


# Check that every contents has a datacube-metadata.yaml
def check_datacube_metadata():
    """
    Checks if all items also have a datacube metadata
    """
    for platform in overview:
        for result in overview[platform]:
            contains_meta = False
            for file in result["contents"]:
                if file["Key"].endswith("datacube-metadata.yaml"):
                    contains_meta = True
            if contains_meta == False:
                print(
                    f"{platform} :: Missing datacube-metadata.yaml in {result['prefix']}"
                )


# Check for error logs
def check_for_error_log():
    log_file_names = ["log_file.txt", "log_file.csv"]
    for platform in overview:
        for result in overview[platform]:
            for file in result["contents"]:
                for log_file_name in log_file_names:
                    if file["Key"].endswith(log_file_name):
                        print(f"{platform} :: Log file found in {result['prefix']}")


# Check for anomolies
def check_for_anomolies():
    """
    Checks for files found less than 10% of the time and if not a known file, prints out the file name

    Does not work for landsat as the file names are not consistent
    """

    # Loop platforms in overview
    for platform in overview:

        if platform.startswith("landsat"):
            continue

        suffixes = {}
        parsed_names = []
        # Loop results in platform
        for result in overview[platform]:
            for file in result["contents"]:
                file_name = (
                    file["Key"]
                    .replace(result["prefix"], "")
                    .replace(result["prefix"].split("/")[-2] + "_", "")
                )

                suffixes[file_name] = suffixes.get(file_name, 0) + 1

                parsed_names.append(
                    {"key": file_name, "prefix": result["prefix"], "url": result["url"]}
                )

        # Get only the ones that appear less than 10 percent of the time and not a known file
        anomolies = []
        for key, value in suffixes.items():
            if value < len(overview[platform]) / 10 and key not in known_files:
                anomolies.append(key)

        # Print out the anomolies
        anomoly_files = [i for i in parsed_names if i["key"] in anomolies]
        if anomoly_files:
            print(f"Found {len(anomoly_files)} anomolies:")
            print("\n".join([f"{i['key']} in {i['prefix']}" for i in anomoly_files]))


# Ensures tiff size is greater than an empty tiff
def check_tiff_size():
    """
    For all tiff files, ensure that the size is over 100 bytes
    """
    for platform in overview:
        for result in overview[platform]:
            for file in result["contents"]:
                if file["Key"].endswith(".tif"):
                    if file["Size"] < 100:
                        print(
                            "Tiff file {} is less than 100 bytes in {}".format(
                                file["Key"], result["url"]
                            )
                        )


# Check that no blank files are in storage
def check_empty_files():
    """
    For all files, ensure that the size is over 100 bytes
    """
    for platform in overview:
        for result in overview[platform]:
            for file in result["contents"]:
                if file["Size"] == 0:
                    print("File {} is empty in {}".format(file["Key"], result["url"]))
