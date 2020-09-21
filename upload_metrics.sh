#!/bin/bash
# Push metrics up to the stats server
SCRIPTS_ENV_FILE=${SCRIPTS_ENV_FILE:-./.env}
METRICS_DIRECTORY=${METRICS_DIRECTORY:-/usr/local/lib/cosmoz-rest-wrapper/metrics}
METRICS_UPLOADKEY=${METRICS_UPLOADKEY:-nokey}
METRICS_UPLOADENDPOINT="${METRICS_UPLOADENDPOINT:-http://lw-78-cdc.it.csiro.au:3001/api/upload}"

. "$SCRIPTS_ENV_FILE"

if cd "$METRICS_DIRECTORY" ; then
  echo "Entered metrics directory"
else
  echo "Cannot enter metrics directory. exiting."
  exit 1
fi

if mkdir -p ./archive ; then
  echo "Archive directory created"
else
  echo "Archive directory could not be created, exiting"
  exit 1
fi

for f in ./*.txt; do
  if gzip -k9 "$f" ; then
    echo "gzipped $f ready to upload"
  else
    echo "could not gzip $f, skipping"
    continue
  fi
  if mv "$f" ./archive ; then
    echo "moved $f to archive dir"
  else
    echo "Could not move $f to archive dir, exiting"
    exit 1
  fi
done
TIME=`date +"%Y-%m-%dT%s"`

for f in ./*.txt.gz; do
  echo "uploading $f..."
  if curl -H "Authorization: Bearer $METRICS_UPLOADKEY" -F "logfilezip=@$f" "$METRICS_UPLOADENDPOINT" -o result.json ; then
    echo "Uploaded."
    RESULT=$(<result.json)
    if [ -z "${RESULT##*"succes"*}" -a -z "${RESULT##*"true"*}" ] ; then
      echo "Seems like a success. Removing the uploaded file."
      rm -f "$f"
    else
      echo 'Seems like not a success.'
    fi
  else
    echo "Didnt upload!"
  fi
  rm -f result.json
done

if cd ./archive ; then
  echo "Entered archive directory"
else
  echo "Could not enter archive directory"
  exit 1
fi

if tar -czf "metrics_$TIME.tar.gz" ./*.txt ; then
  echo "Compressed archive logs successfully."
  rm -f ./*.txt
else
  echo "Could not compress archive logs."
fi
