#!/bin/python

# Runs upload measures
# Requires a raster TIFF file
# It is important to run it inside a Google VM because the files to work are larger.
import argparse, json, logging, re, subprocess, time

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
from jinja2 import Environment, FileSystemLoader

# Default setup
logger = logging.getLogger()
default_gcs = 'gs://scratch-matias/distance_from_port/distance-from-port.xyz'
default_dataset = 'scratch_matias_ttl_60_days'
translated_path = 'distance-from-port.xyz'
templates = Environment(
    loader=FileSystemLoader("assets"),
)

def prompt_accepted(prompt:str, strict:bool = True):
    """
    If strict only accepts yes to continue, if not returns the value
    """
    r = prompt.strip().lower() in ['yes','y','true','']
    if not strict:
        return r
    assert r

def download_raster(render_path, destination_file_name):
    """Downloads a render file from the bucket."""
    gcs_render_path = re.search(r'gs://([^/]*)/(.*)', render_path)
    bucket_name = gcs_render_path.group(1)
    source_blob_name = gcs_render_path.group(2)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logger.info(f"Downloaded storage object {source_blob_name} from bucket {bucket_name} to local file {destination_file_name}.")

def upload_blob(source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    gcs_render_path = re.search(r'gs://([^/]*)/(.*)', destination_blob_name)
    bucket_name = gcs_render_path.group(1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def create_tables_if_not_exists(destination_table, labels):
    """Creates tables if they do not exists.
    If it doesn't exist, create it. And if exists, deletes the data of date range.

    :param client: Client of BQ.
    :type client: BigQuery.Client.
    :param destination_table: dataset.table of BQ.
    :type destination_table: str.
    :param labels: the label of the dataset. Default None.
    :type labels: dict.
    """
    client = bigquery.Client()
    destination_table_ds, destination_table_tb = destination_table.split('.')
    destination_dataset_ref = bigquery.DatasetReference(client.project, destination_table_ds)
    destination_table_ref = destination_dataset_ref.table(destination_table_tb)
    try:
        table = client.get_table(destination_table_ref) #API request
        logger.info(f'Ensures the table [{table}] exists.')
        #deletes the content
        query_job = client.query(
            f"""
               DELETE FROM `{ destination_table }`
            """,
            bigquery.QueryJobConfig(use_query_cache=False,use_legacy_sql=False,labels=labels)
        )
        logger.info(f'Delete Job {query_job.job_id} is currently in state {query_job.state}')
        result = query_job.result()
        logger.info('Deleted.')
    except BadRequest as err:
        logger.error(f'Bad request received {err}.')
    except NotFound as err:
        table = bigquery.Table(destination_table_ref, schema=schema_json2builder())
        table.clustering_fields = ["gridcode"]
        table = client.create_table(table)
        logger.info(f'Table {destination_table_ds}.{destination_table_tb} created.')
    except Exception as err:
        logger.error(f'Unrecongnized error: {err}.')



def run_upload_measures(arguments):
    parser = argparse.ArgumentParser(description='Run upload  spatial_measures')
    parser.add_argument('-i','--render_path', help='The path to the render image (Format str).', required=True)
    parser.add_argument('-gcs_temp','--gcs_temporal', help='Temporal path to upload the distance from port and use to upload to BQ (Format str).', required=False, default=default_gcs)
    parser.add_argument('-dfp','--dist_from_port', help='The BQ source distance from port table (Format str, ex: datset.table).',
                        required=False, default='{default_dataset}.distance_from_port')
    parser.add_argument('-dfs','--dist_from_shore', help='The BQ source distance from shore table (Format str, ex: datset.table).',
                        required=False, default='{default_dataset}.distance_from_shore')
    parser.add_argument('-bth','--bathymetry', help='The BQ source of bathymetry table (Format str, ex: datset.table).',
                        required=False, default='{default_dataset}.bathymetry')
    parser.add_argument('-o','--destination_table', help='The BQ destination table (Format str, ex: project.datset.table).',
                        required=False, default='{default_dataset}.spatial_measures_clustered')
    parser.add_argument('-labels','--labels', help='Adds a labels to a table (Format: json).', required=False, default={'environment': 'development', 'project': 'core_pipeline', 'resource_creator': 'matias', 'stage': 'development', 'step': 'pre-measure', 'version': 'v3'}, type=json.loads)
    args = parser.parse_args(arguments)

    start_time = time.time()

    render_path = args.render_path
    gcs_temp = args.gcs_temporal
    dist_from_port = args.dist_from_port
    dist_from_shore = args.dist_from_shore
    bathymetry = args.bathymetry
    destination_table = args.destination_table
    labels = args.labels

    # 1. If raster is in GCS, the raster file is downloaded in the container.
    # raster file is about 2,5G consider the space.
    if re.match('gs://',render_path):
        local_path = f'/app/{re.search(r"/(.*)$", relative_render_path).group(1)}'
        download_raster(gcs_render_path.group(1), relative_render_path, local_path)
        render_path = local_path
    #Assuming the render file is local.
    logger.info(f'Render path is <{render_path}>.')

    # 2. Runs the GDAL info of the raster.
    logger.info(f'Checks the GDAL information of <{render_path}>.')
    gdalinfocmd = subprocess.run(['gdalinfo', render_path], capture_output=True)
    logger.info(gdalinfocmd.stdout.decode())
    gdalinfojsoncmd = subprocess.run(['gdalinfo', '-json', render_path], capture_output=True)
    gdalinfojson = json.loads(gdalinfojsoncmd.stdout.decode())

    # 3. Checks the GEO coordinate system type that TIFF uses.
    tiff_not_reaching_requirements=False
    tiff_system = gdalinfojson['stac']['proj:projjson']['name']
    logger.info(f'Checks if the TIFF <{render_path}> is using coordinate system WGS84 (World Geodetic System 84).')
    if tiff_system.replace(' ','') != 'WGS84':
        logger.error('==> TIFF should be using WGS84 system.')
        prompt_accepted(input('#  Do you want to proceed in converting the TIFF to WGS84? (y/n)?'))
        tiff_not_reaching_requirements=True
    else:
        logger.info('==> TIFF is using WGS84.')

    # 4. Checks the 0.01 degree spacing
    degree_spacing = gdalinfojson['geoTransform'][1]
    logger.info(f'Checks if the TIFF <{render_path}> has the 0.01 degree spacing.')
    if degree_spacing != 0.01:
        logger.error('==> TIFF should be using 0.01 degree spacing.')
        prompt_accepted(input('#  Do you want to proceed in converting the TIFF to 0.01 degree spaced? (y/n)?'))
        tiff_not_reaching_requirements=True
    else:
        logger.info('==> TIFF is 0.01 degree spacing.')


    # 5. If the raster is not already structured this way, it can be warpped.
    if tiff_not_reaching_requirements:
        aux_path = render_path+'_aux'
        gdalwarpcmd = subprocess.run([
            'gdalwarp',
            '-t_srs','EPSG:4326', # Set target spatial reference this is for WGS84
            '-r', 'bilinear', # Resampling method to use. bilinear resampling.
            '-te', '-180.005','-90.005','179.995','90.005', # Set georeferenced extents of output file to be created.
            '-ts', '36000','18001', # Set output file size in pixels and lines (width and height)
            '-overwrite', render_path, render_path # Overwrite the target dataset if it already exists. Overwriting must be understood here as deleting and recreating the file from scratch.
        ], capture_output=True)
        logger.info(gdalwarpcmd.stdout.decode())
        prompt_accepted(input(f'#  Result in path <{aux_path}>. Do you want to continue using this new file as original? (y/n)?'))
        render_path = aux_path


    # 6. Test the value at some geographic location.
    logger.info('Checks the value indicated long and lat points using WGS84. Example (long, lat) = (4,4)')
    gdallocationinfocmd = subprocess.run(['gdallocationinfo', '-wgs84', render_path, '4', '4'], capture_output=True)
    logger.info(gdallocationinfocmd.stdout.decode())

    gdallocationinfovalcmd = subprocess.run(['gdallocationinfo', '-wgs84', '-valonly', render_path, '4', '4'], capture_output=True)
    logger.info(f'The value is {gdallocationinfovalcmd.stdout.decode()}... Consider the units are in kilometers.')

    # 7. Request to change units: kms to mts.
    if prompt_accepted(input(f'#  Do you want to change values from kilometers to meters (y/n)?'), False):
        render_path_meters = render_path.replace('.','_m.')
        gdalcalccmd = subprocess.run(['gdal_calc.py', '-A', render_path, f'--outfile={render_path_meters}', '--calc="A*1000"'], capture_output=True)
        gdallocationinfovalcmd = subprocess.run(['gdallocationinfo', '-wgs84', '-valonly', render_path_meters, '4', '4'], capture_output=True)
        logger.info(f'The value is {gdallocationinfovalcmd.stdout.decode()}... Consider the units of <{render_path_meters}> are in meters.')
        render_path = render_path_meters

    # 8. Converts raster file to a text file saved in local.
    logger.info('Now we need to convert the tiff to a text format that we can load in to bigquery.\nIMPORTANT: Note that the exported text file would be pretty huge!')
    gdaltranslatecmd = subprocess.Popen([
        'gdal_translate',
        '-of', 'XYZ', render_path, '/vsistdout/'
    ], stdout=subprocess.PIPE)
    gdaltranslatecmd.wait()
    with open(translated_path, "w") as outfile:
        awkcmd = subprocess.run([
            'awk', "\"{printf \"%.3f %.3f %.3f\n\", $1, $2, $3}\""
        ], stdin=gdaltranslatecmd.stdout, check=True, stdout=outfile)
    logger.info(f'Result in path <{translated_path}>.')

    # 9. Uploads text file to GCS.
    if prompt_accepted(input('#  Do you want to upload the file to GCS? (y/n)?'), False):
        upload_blob(translated_path, f'{gcs_temp}/{translated_path})

    # 10. Run the SQL query to merge layers.
    logger.info('Using the text file in GCS, we proceed to run the sql query to merge layers.')
    query_template = templates.get_template('spatial_measures.sql.j2')
    query = query_template.render(
        distance_from_shore=dist_from_shore,
        distance_from_port=dist_from_port,
        bathymetry=bathymetry,
    )


    ### ALL DONE
    logger.info(f'All done, you can find the output: {args.destination_table}')
    logger.info(f'Execution time {(time.time()-start_time)/60} minutes')
