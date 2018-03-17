#!/bin/bash

VBOX_PATH="/cygdrive/c/Program Files/Oracle/VirtualBox"

function isRunning() {
	local vmName=$1

	manage showvminfo ${vmName} | grep -c "running (since"
}

function manage() {
	echo "${VBOX_PATH}/VBoxManage.exe" "$@"
	"${VBOX_PATH}/VBoxManage.exe" "$@"
}

function stopVm() {
	local vmName=$1

	manage controlvm ${vmName} acpipowerbutton

	while [[ $(isRunning ${vmName}) == "1" ]]; do
		>&2 echo "Polling for shutdown of ${vmName}..."
		sleep 5s
	done

	>&2 echo "${vmName} stopped"
}

function startVm() {
	local vmName=$1
	manage startvm ${vmName}
}

function restoreSnapshot() {
	local vmName=$1
	local snapshotName=$2

	echo ${snapshotName}

	manage snapshot ${vmName} restore "${snapshotName}"
}

function main() {

    while getopts ":v:a:s:" opt; do
        case ${opt} in
            v)
                vmName=${OPTARG}
                ;;
            a)
                action=${OPTARG}
                ;;
            s)
                snapshotName=${OPTARG}
                ;;
        esac
    done

    echo "vm: ${vmName}, snapshotName: ${snapshotName}, action: ${action}"

    case ${action} in
        "start")
            if [[ $(isRunning ${vmName}) == "0" ]]; then
			    startVm ${vmName}
		    fi
		;;
		"restart")
		    if [[ $(isRunning ${vmName}) == "1" ]]; then
			    stopVm ${vmName}
		    fi
		    startVm ${vmName}
        ;;
        "restore")
            stopVm ${vmName}
            restoreSnapshot ${vmName} "${snapshotName}"
            startVm ${vmName}
        ;;
	esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi