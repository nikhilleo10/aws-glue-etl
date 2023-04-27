if [ ! -f "./data-eng-updated/data-eng-updated-new.json" ]; then
echo Error: data-eng-updated-new.json File does not exist
exit 1
fi

S3_FILE_URL=$1

if [ ! $S3_FILE_URL ]; then
echo Error: No S3 Upload path found in the workflow, please verify the actions.
exit 1
fi

echo Starting the update script.
echo

cd_properties_file='./data-eng-updated/data-eng-updated-new.json'

S3_UPLOAD_PATH=$(jq -r '.command.scriptLocation' $cd_properties_file)

echo $S3_UPLOAD_PATH

yq -i '.command.scriptLocation="'$S3_FILE_URL'"' $cd_properties_file -o json

S3_UPLOAD_PATH=$(jq -r '.command.scriptLocation' $cd_properties_file)

echo $S3_UPLOAD_PATH
