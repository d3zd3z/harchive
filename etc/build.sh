#! /bin/sh

# Build the project, assumes we're in the active directory.
set -e
AD=`pwd -P`
mkdir -p ${AD}/dist
echo '[]' > ${AD}/dist/package.conf
echo 'Configuring'
runghc Setup.hs configure --package-db=${AD}/dist/package.conf
echo 'Building'
runghc Setup.hs build
runghc Setup.hs register --inplace
