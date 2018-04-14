#!/usr/bin/env bash

VMS_DIR="/cygdrive/c/Users/m/VirtualBox VMs"
INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function do_recover() {
    cd "${VMS_DIR}/${i}"
    if [[ $(ls | grep -c vbox-prev) == "1" ]]; then
        DATE=`date '+%Y_%m_%d-%H_%M_%S'`
        mv "${1}.vbox" "${DATE}.broke.${1}.vbox"
        mv "${i}.vbox-prev" "${i}.vbox"
        echo "Recovered ${1}"
    else
        echo "ERR: unable to recover ${1}"
    fi
}

function recover_from_stall() {
    for i in $(ls "${VMS_DIR}" | egrep ^win | grep -v base);do
        set +e
        manage showvminfo "${i}" > /dev/null
        if [[ $? -ne 0 ]]; then
            echo "${i} needs restoring"
            do_recover ${i}
        else
            echo "${i} OK"
        fi
        set -e
    done
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    source "${INSTALL_DIR}/vbox-ctrl.sh"
    recover_from_stall "$@"
fi