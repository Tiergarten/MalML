#!/bin/bash
set -e

VBOX_PATH="/cygdrive/c/Program Files/Oracle/VirtualBox"

function info() {
    echo "[$$] [INFO] $@"
}

function getStatus() {
    local vmName=$1

    STATUS=$(manage showvminfo ${vmName} | grep State | sed 's/  */ /g' | cut -d ' ' -f 2)
    echo ${STATUS}
}

function isRunning() {
	local vmName=$1

	isStatus ${vmName} "running"
}

function isStatus() {
    local vmName=$1
    local status=$2

    if [[ $(getStatus ${vmName}) == "${status}" ]]; then
        echo 1
    else
        echo 0
    fi
}

function manage() {
	info "${VBOX_PATH}/VBoxManage.exe" "$@"
	"${VBOX_PATH}/VBoxManage.exe" "$@"
	retVal=$?

	if [[ "${retVal}" -ne "0" ]]; then
	    echo "WARN: looks like there is a problem with Vm ${3}"
	fi

	return ${retVal}
}

function stopVm() {
	local vmName=$1

	if [[ "$(isStatus ${vmName} 'paused')" == "1" ]]; then
	    resumeVm ${vmName}
    fi

	if [[ "$(isRunning ${vmName})" -ne "1" ]];then
	    echo "WARN: vm not running, returning..."
	    return
    fi

    info "Sending stop signal to vm ${vmName}"
	manage controlvm ${vmName} poweroff

    counter=0
	while [[ $(isRunning ${vmName}) == "1" ]]; do

	    if [[ "${counter}" -gt 15 ]]; then
	        info "failed to stop vm ${vmName}"
	        exit 1
	    fi

	    if [[ "${counter}" -ne 0 && $((${counter} % 5)) == 0 ]];then
	        info "Re-sending stop signal to vm ${vmName} after ${counter} sleeps..."
	        manage controlvm ${vmName} poweroff
        fi

		info "Polling for shutdown of ${vmName}..."
		sleep 10s
		counter=$((counter+1))
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

function resumeVm() {
    local vmName=$1

    echo "WARN: unpausing vm...."
    manage controlvm ${vmName} resume
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
        "status")
            if [[ $(isRunning ${vmName}) == "0" ]]; then
                echo "STOPPED"
            else
                echo "RUNNING"
            fi
        ;;
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