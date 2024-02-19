from rucio.client.didclient import DIDClient

# Initialize Rucio client
did_client = DIDClient()
    
def find_files_in_scope(scope):
    '''
    For a given scope, find and return all FILE objects within it.

    Returns a list of dicts, with "scope', "name" and "type" attributes.
    '''
    try:
        # List all files in the scope
        files = did_client.scope_list(scope)
        desired_keys = ["scope", "name", "type"]
        return [{key: d[key] for key in desired_keys if key in d} for d in files if d["type"] == "FILE"]
    except Exception as e:
        print(f"Failed to list files in scope {scope}: {str(e)}")
        return []

if __name__ == "__main__":
    # Specify the Rucio scope and dataset name
    scope = "vlass"
    dataset_name = "vlass"

    # Find all files in the scope
    files = find_files_in_scope(scope)

    # Attach files to the dataset
    for file in files:
        try:
            did_client.attach_dids(scope, dataset_name, [file])
        except Exception as e:
            print(f"Failed to attach {file} to the dataset: {str(e)}")

