#!/bin/bash

VBOX_PATH="/cygdrive/c/Program Files/Oracle/VirtualBox"

function info() {
    echo "[$$] [INFO] $@"
}

function isRunning() {
	local vmName=$1

	manage showvminfo ${vmName} | grep -c "running (since"
}

function manage() {
	info "${VBOX_PATH}/VBoxManage.exe" "$@"
	"${VBOX_PATH}/VBoxManage.exe" "$@"
}

function stopVm() {
	local vmName=$1

	manage controlvm ${vmName} acpipowerbutton

	while [[ $(isRunning ${vmName}) == "1" ]]; do
		info "Polling for shutdown of ${vmName}..."
		sleep 5s
	done

	info "${vmName} stopped"
}

function startVm() {
	local vmName=$1
	manage startvm ${vmName}
}

function restoreSnapshot() {
	local vmName=$1
	local snapshotName=$2

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

    info "VBOX-CTRL CALLED: vm: ${vmName}, snapshotName: ${snapshotName}, action: ${action}"

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
		    sleep 5s
		    startVm ${vmName}
        ;;
        "restore")
            stopVm ${vmName}
            sleep 5s
            restoreSnapshot ${vmName} "${snapshotName}"
            sleep 5s
            startVm ${vmName}
        ;;
	esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi