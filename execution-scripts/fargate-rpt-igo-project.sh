#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu

cat<<EOF

 ▄▀▀▄▀▀▀▄  ▄▀▀▀▀▄      ▄▀▀▄▀▀▀▄  ▄▀▀█▄▄▄▄  ▄▀▀▄▀▀▀▄      ▄▀▀▀▀▄   ▄▀▀█▄▄▄▄  ▄▀▀▄ ▀▄ 
█   █   █ █ █   ▐     █   █   █ ▐  ▄▀   ▐ █   █   █     █        ▐  ▄▀   ▐ █  █ █ █ 
▐  █▀▀█▀     ▀▄       ▐  █▀▀█▀    █▄▄▄▄▄  ▐  █▀▀▀▀      █    ▀▄▄   █▄▄▄▄▄  ▐  █  ▀█ 
 ▄▀    █  ▀▄   █       ▄▀    █    █    ▌     █          █     █ █  █    ▌    █   █  
█     █    █▀▀▀       █     █    ▄▀▄▄▄▄    ▄▀           ▐▀▄▄▄▄▀ ▐ ▄▀▄▄▄▄   ▄▀   █   
▐     ▐    ▐          ▐     ▐    █    ▐   █             ▐         █    ▐   █    ▐   
                                 ▐        ▐                       ▐        ▐        
                                                   
EOF

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  rpt-igo-project

}