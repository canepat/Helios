#!/usr/bin/env bash

sudo -S sysctl net.core.rmem_max=2097152 > /dev/null
sudo -S sysctl net.core.wmem_max=2097152 > /dev/null

java -Xms2G -Xmx4G -XX:BiasedLockingStartupDelay=0 \
-Daeron.mtu.length=16384 -Daeron.rcv.buffer.length=16384 -Daeron.socket.so_sndbuf=2097152 \
-Daeron.socket.so_rcvbuf=2097152 -Daeron.rcv.initial.window.length=2097152 -Dagrona.disable.bounds.checks=true \
-cp ".:../build/production/Helios:../build/test/Helios:../lib/*" \
echo.EchoEmbeddedIpc