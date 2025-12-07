gcloud beta container \
    --project \
"trusty-anchor-473006-u9" clusters create-auto "postgre-cluster" \
    --region \
"europe-west9" \
    --release-channel \
"stable" \
    --enable-ip-access \
    --enable-master-global-access \
    --no-enable-google-cloud-access \
    --network \
"projects/trusty-anchor-473006-u9/global/networks/default" \
    --subnetwork \
"projects/trusty-anchor-473006-u9/regions/europe-west9/subnetworks/default" \
    --binauthz-evaluation-mode=DISABLED \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/trace.append

gcloud container clusters get-credentials postgre-cluster --region europe-west9 --project trusty-anchor-473006-u9 