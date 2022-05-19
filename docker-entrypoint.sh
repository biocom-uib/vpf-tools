#!/bin/bash

VPF_DATA_URL='https://bioinfo.uib.es/~recerca/VPF-Class/vpf-class-data.tar.gz'
VPF_DATA_PATH='/opt/vpf-tools/vpf-data'


download_vpf_class_data() {
    mkdir -p "$VPF_DATA_PATH"

    (
        cd "$VPF_DATA_PATH";
        curl "$VPF_DATA_URL" | tar --overwrite --no-same-owner --strip-components=1 -xzf -;
    )

    [ "$VPF_TOOLS_CHMOD" == 0 ] || chmod -R a+rwX "$VPF_DATA_PATH"/*

    touch "$VPF_DATA_PATH/last-update"
}


check_and_update_vpf_class_data() {
    if [ -n "$VPF_DATA_AUTOUPDATE" ] && [ "$VPF_DATA_AUTOUPDATE" == 0 ]
    then
        echo "VPF-Class data autoupdate is disabled."

    elif [ ! -f "$VPF_DATA_PATH/last-update" ]
    then
        echo "VPF-Class data files not found, downloading"
        download_vpf_class_data

    else
        local local_date
        local_date="$(stat -c %Y "$VPF_DATA_PATH/last-update")"

        if [ $? != 0 ]
        then
            echo "Error: Could not stat $VPF_DATA_PATH/index.yaml, but it is present. Do you have correct permissions?"
            return 1
        fi

        local remote_date
        remote_date="$(curl -s -v -X HEAD "$VPF_DATA_URL" 2>&1 | grep '^< Last-Modified: ')"
        remote_date="${remote_date##< Last-Modified: }"

        if [ $? != 0 ]
        then
            echo "Warning: Failed to test remote modification date: $VPF_DATA_URL. Overwriting"
            download_vpf_class_data

        elif [ "$(date -d "$remote_date" +%s)" -gt "$local_date" ]
        then
            echo "VPF-Class data files are outdated, updating"
            download_vpf_class_data

        else
            echo "VPF-Class data files are up-to-date."
        fi
    fi
}

[ "$VPF_TOOLS_CHMOD" == 0 ] || umask 0000

check_and_update_vpf_class_data

"$@"
