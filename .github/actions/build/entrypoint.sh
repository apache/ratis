#!/bin/sh -l
ls -lah
whoami
ls -lah target
./dev-support/checks/${TEST_TYPE:-build}.sh
