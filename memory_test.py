import os
import tempfile, shutil
import json

# Create dummy content
dummy_content = {"ID" : 1, 
                "Name" : "Rafi Adyatma",
                "Email" : "rafi.adyatma@gmail.com"
                }

temp_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
file_name = temp_file.name

# Write to file
# f.write('lmao')
json.dump(dummy_content, temp_file)

# Read to file
temp_file.seek(0)
file_content = json.load(temp_file)

temp_file.close()
# shutil.copy(file_name, 'joinTemp.txt')

print(temp_file)
print(file_content['ID'])

os.remove(file_name)