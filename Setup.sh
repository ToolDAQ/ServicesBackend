#!/bin/bash
export PS1='${debian_chroot:+($debian_chroot)}\[\033[35;2;1m\]\u@\h\[\033[00m\]:\[\033[00;36m\]\w\[\033[00m\]\$ '

#Application path location of applicaiton
Dependencies=/opt

#source ${ToolDAQapp}/ToolDAQ/root/bin/thisroot.sh

export LD_LIBRARY_PATH=`pwd`/lib:${Dependencies}/zeromq-4.0.7/lib:${Dependencies}/boost_1_66_0/install/lib:${Dependencies}/ToolFrameworkCore/lib:${Dependencies}/ToolDAQFramework/lib:$LD_LIBRARY_PATH

export SEGFAULT_SIGNALS="all"
