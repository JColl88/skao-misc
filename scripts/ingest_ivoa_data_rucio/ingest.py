import os
import requests
import urllib3

import progressbar
import pprint
import urllib.request
from rucio.client.didclient import DIDClient
from rucio.client.uploadclient import UploadClient

DATASET_PREFIX='RAC'
ATTEMPT='001'
RUCIO_SCOPE='testing_ingest_180723'
RUCIO_RSE='STFC_STORM_ND'
RUCIO_LIFETIME=31560000
PFN_BASE_PATH='https://srcdev.skatelescope.org:443/storm/sa/test_rse/dev/nondeterministic'
CADC_OBSCORE_URL='https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus/sync?LANG=ADQL&FORMAT=csv&QUERY=select%20*%20from%20ivoa.ObsCore%20C%20join%20ivoa.ObsFile%20F%20on%20C.core_id%20=%20F.core_id%20where%20obs_collection=%20%27RACS%27'
LIMIT=250

TRY_GET_DATA=True
DO_UPLOAD_AND_REGISTER=True
ADD_METADATA=True

# ---

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RUCIO_DATASET_NAME='{}_{}'.format(DATASET_PREFIX, ATTEMPT)
RUCIO_NAME_PREFIX='{}_'.format(ATTEMPT)
PFN_BASE_PATH_WITH_SCOPE='{}/{}'.format(PFN_BASE_PATH, RUCIO_SCOPE)

# progress bar for accessing data from datalink
pbar = None
def show_progress(block_num, block_size, total_size):
    global pbar
    if pbar is None:
        pbar = progressbar.ProgressBar(maxval=total_size)
        pbar.start()

    downloaded = block_num * block_size
    if downloaded < total_size:
        pbar.update(downloaded)
    else:
        pbar.finish()
        pbar = None

# get data from obscore result
obscore_table = requests.get(CADC_OBSCORE_URL).text

metadata = []
metadata_headers = []
for idx, line in enumerate(obscore_table.split('\n')):
    line_parsed = [entry.strip() for entry in line.split(',')]
    if idx==0:
        metadata_headers = line_parsed
        continue
    metadata_line = {}
    for key, value in zip(metadata_headers, line_parsed):
        if value:
            metadata_line[key] = value
    if all((metadata_line.get('calib_level'), metadata_line.get('obs_collection'), metadata_line.get('obs_id'), metadata_line.get('obs_publisher_did'))):
        metadata.append(metadata_line)

# fix s_region
for entry in metadata:
    s_region = entry.get('s_region')
    if s_region:
        if s_region.startswith('polygon'):
           s_region_parsed = '{{{}}}'.format(
                   str(
                       [('{}d'.format(x), '{}d'.format(y)) for x, y in zip(s_region.split()[1::2], s_region.split()[2::2])]
                       ).lstrip('[').rstrip(']')
                   ).replace('\'', '')
           entry['s_region'] = s_region_parsed

# make access url point to rucio datalink service
for entry in metadata:
    entry['cadc_access_url'] = entry['access_url']
    rucio_name = '{}{}'.format(RUCIO_NAME_PREFIX, entry['obs_id']).replace('+', 'p')
    rucio_did = '{}:{}'.format(RUCIO_SCOPE, rucio_name)
    entry['access_url'] = "https://ivoa.datalink.srcdev.skao.int/rucio/links?id={}".format(rucio_did)

# remove any non-obscore fields
for entry in metadata:
    entry.pop('uri')
    entry.pop('core_id')
    entry.pop('lastModified')

# get total # of files
print("total # of files: {}".format(len(metadata)))

# get total file sizes
size = 0
for entry in metadata:
    size += int(entry.get('content_length'))
print("total size of dataset: {}GB".format((size/(10**9))))

# evaluate cadc datalink response and get file
data_paths = []
for entry in metadata[:LIMIT]:
    rucio_name = entry['obs_id']
    if not os.path.exists(rucio_name):
        if TRY_GET_DATA:
            try:
                output = requests.get(entry['cadc_access_url'])
                if output.status_code == 200:
                    output_byrow = [row.strip() for row in output.text.split('\r\n')]
                    access_url_from_datalink_response_idx = output_byrow.index('<TD>#this</TD>') - 3
                    access_url_from_datalink_response = output_byrow[access_url_from_datalink_response_idx].lstrip('<TD>').rstrip('</TD>')
                    print("retrieving file of size {}GB from {}".format(int(entry['content_length'])/10**9, access_url_from_datalink_response))
                    urllib.request.urlretrieve(access_url_from_datalink_response, rucio_name, show_progress)
            except Exception as e:
                print(e)
                rucio_name = None
        else:
            rucio_name = None
    else:
        print("already have file {}, skipping try_get_data".format(rucio_name))
    data_paths.append(rucio_name)

# get list of already processed files (for idempotency)
processed_files = []
if os.path.exists('registered_files'):
   with open("registered_files", 'r') as f:
      contents = f.read()
      processed_files = contents.split('\n')

# upload & register file, appending to the list of processed files on success
with open("registered_files", 'a') as f:
    if DO_UPLOAD_AND_REGISTER:
        uploadclient=UploadClient()
        didclient=DIDClient()
        try:
            didclient.add_dataset(scope=RUCIO_SCOPE, name=RUCIO_DATASET_NAME, lifetime=RUCIO_LIFETIME)
        except Exception as e:
            print(e)
        for data_entry, metadata_entry in zip(data_paths[:LIMIT], metadata[:LIMIT]):
            if data_entry:
                rucio_name = '{}{}'.format(RUCIO_NAME_PREFIX, metadata_entry['obs_id']).replace('+', 'p')
                rucio_did = '{}:{}'.format(RUCIO_SCOPE, rucio_name)
                if rucio_did in processed_files:
                    print("already processed {}, skipping register&upload".format(rucio_did))
                else:
                    print("will upload&register did {}".format(rucio_did))
                    upload_item = [{
                        'path': metadata_entry['obs_id'],
                        'rse': RUCIO_RSE,
                        'did_name': rucio_name,
                        'did_scope': RUCIO_SCOPE,
                        'pfn': "{}/{}".format(PFN_BASE_PATH_WITH_SCOPE, rucio_name),
                        'register_after_upload': True,
                        'transfer_timeout': 3600
                    }]
                    pprint.pprint(upload_item)
                    try:
                        uploadclient.upload(items=upload_item)
                        # attach to dataset
                        attachment = {
                            "scope": RUCIO_SCOPE,
                            "name": RUCIO_DATASET_NAME,
                            "dids": [{
                                "scope": RUCIO_SCOPE,
                                "name": rucio_name
                            }]
                        }
                        didclient.attach_dids_to_dids(attachments=[attachment])
                    except Exception as e:
                        print(e)
                        continue
                    f.write("{}\n".format(rucio_did))
                    f.flush()

# add metadata to files
if ADD_METADATA:
    for data_entry, metadata_entry in zip(data_paths[:LIMIT], metadata[:LIMIT]):
        if data_entry:
            rucio_name = '{}{}'.format(RUCIO_NAME_PREFIX, metadata_entry['obs_id']).replace('+', 'p')
            rucio_did = '{}:{}'.format(RUCIO_SCOPE, rucio_name)
            print("setting metadata for did {}".format(rucio_did))
            didclient.set_metadata_bulk(scope=RUCIO_SCOPE, name=rucio_name, meta=metadata_entry)

