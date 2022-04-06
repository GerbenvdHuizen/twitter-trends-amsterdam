#!/usr/bin/env bash

set -euvo pipefail

SCRIPT_DIR="./"


mkdir -p $SCRIPT_DIR/local_pypi
mkdir -p $SCRIPT_DIR/local_pypi_zip

pip install --upgrade pip
poetry build
pip wheel -w local_pypi $SCRIPT_DIR/dist/*.whl

pip cache purge

for F in local_pypi/*.whl
do
    NAME=$(echo $F | sed "s/-.*//" | sed "s/local_pypi\///" | sed "s/_/-/g")
    mkdir -p local_pypi/simple/${NAME}
    mv "${F}" local_pypi/simple/${NAME}
done

for D in local_pypi/simple/*
do
    echo '<html><head><meta name="pypi:repository-version" content="1.0"></head><body>' > $D/index.html
    for F in $D/*.whl
    do
        FILE=$(echo $F | sed "s/.*\///")
        echo "<a href=\"./${FILE}\">${FILE}</a><br/>" >> $D/index.html
    done
    echo '</body></html>' >> $D/index.html
done

rm -rf local_pypi_zip/local_pypi.zip

zip -r local_pypi_zip/local_pypi.zip local_pypi

rm -rf local_pypi
