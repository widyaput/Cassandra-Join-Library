import os
import tempfile, shutil
import json


def create_tempfile():
    # Create new temp file to store temporary result.
    # Returns temporaryFileWrapper
    temp_file = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    return temp_file


def save_to_tempfile(temp_file, data):
    # Store data / content to tempfile.
    # Returns Boolean
    # True when store data successfully, Otherwise is False
    try :
        json.dump(data, temp_file)
    except :
        print("Failed to store data to temporary file!")
        return False

    return True


def load_from_tempfile(temp_file):
    # Load data from a tempfile.
    # Return content of file (Stored data)
    temp_file.seek(0)
    content = json.load(temp_file)

    return content


def close_tempfile(temp_file):
    # Close a tempfile.
    # Returns Boolean
    # True when close tempfile successfully, Otherwise is False

    try :
        filename = temp_file.name
        temp_file.close()
        os.remove(filename)
    except :
        print("Failed to close tempfile!")
        return False

    return True