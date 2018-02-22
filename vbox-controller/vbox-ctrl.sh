#!/bin/bash

VBOX_PATH="/cygdrive/c/Program Files/Oracle/VirtualBox"

function isRunning() {
	local vmName=$1

	manage showvminfo ${vmName} | grep -c "running (since"
}

function manage() {
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

	manage snapshot ${vmName} restore ${snapshotName}
}

function main() {
	local vmName="XP1"

	while true; do
		if [[ $(isRunning ${vmName}) == "0" ]]; then
			startVm ${vmName}
		fi

		sleep 2m

		stopVm ${vmName}
		restoreSnapshot ${vmName} init
	done

}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi