docker build -t glue-con .

WORKSPACE_LOCATION=/Users/apple/Desktop/data-eng-etl/data-eng-updated-new
SCRIPT_FILE_NAME=data-eng-updated-new.py
UNIT_TEST_FILE_NAME=test_notes_transformation.py
# mkdir -p ${WORKSPACE_LOCATION}/tests
# vim ${WORKSPACE_LOCATION}/tests/${UNIT_TEST_FILE_NAME}



docker run -it -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pytest glue-con -c "python3 -m pytest"