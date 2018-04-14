#!/usr/bin/env bash

while [[ $(ps -W | grep -ic VBox) -gt "0" ]]; do
    for i in $(ps -W | grep -i VBox | sed 's/   */ /'g | cut -d ' ' -f 2);do echo "killing $i..."; taskkill /F /PID $i;done
done