#!/bin/bash

echo "Disabling CKAN debug mode..."

sed -i 's/^debug = true/debug = false/' /srv/app/ckan.ini

grep -q "^ckan.debug" /srv/app/ckan.ini \
  && sed -i 's/^ckan.debug.*/ckan.debug = false/' /srv/app/ckan.ini \
  || echo "ckan.debug = false" >> /srv/app/ckan.ini

echo "Cleaning CKAN assets..."
ckan -c /srv/app/ckan.ini asset clean

echo "Building CKAN assets..."
ckan -c /srv/app/ckan.ini asset build