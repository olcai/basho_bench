#!/bin/bash

# Duration is only used for micro benchmark
DURATION=5
BACKEND=file
CONCURRENT=( 1 2 5 10 20 50 )
# Val_size is only used for micro benchmark
VAL_SIZE=( 1 1000 100000 1000000 16000000 100000000 )
# The number of seconds between benchmark runs
SLEEP_BETWEEN=5
# Set SUITE to macro or micro to run it
SUITE=micro


function info-print {
    declare n=''; test "$1" = '-n' && { n='-n'; shift; }
    echo -e $n "---> Benchmark Suite: ${@}"
}

# $1=file $2=duration $3=concurrent $4=val_size $5=backend
# $6=operation $7=filetable_input
function set-opts {
    sed -i 's/{duration, .*}/{duration, '$2'}/g' $1
    sed -i 's/{concurrent, .*}/{concurrent, '$3'}/g' $1
    sed -i 's/{fixed_bin, .*}}/{fixed_bin, '$4'}}/g' $1
    sed -i 's/{backend, .*}/{backend, '$5'}/g' $1
    sed -i 's/{operations, .*}/{operations, [{'$6', 1}]}/g' $1
    sed -i 's;{filetable_input, .*};{filetable_input, "'$7'"};g' $1
}

# $1=duration $2=concurrent $3=val_size $4=operation
# $5=filetable_input $6=bench_type
function run-bench {
    info-print "Starting $6 benchmark with operation $4"
    CONF=configs/current.config
    cp configs/template.config $CONF
    set-opts $CONF $1 $2 $3 $BACKEND $4 $5
    echo "%%INFO Backend: $BACKEND, Benchmark: $6, Duration: $1," \
        " Workers: $2, ValSize: $3, Operation: $4" >> $CONF
    ./basho_bench $CONF
    sleep $SLEEP_BETWEEN
}

# Macro bench cycle
# $1=duration $2=concurrent $3=val_size $4=bench_type
function run-bench-cycle {
    info-print "Starting $4 cycle with $2 concurrent worker(s) and val_size $3"
    # Check if macro or micro benchmark. Use the corresponding put
    # function.
    if [ $4 == "macro" ]; then
        run-bench $1 $2 $3 "put_direct" "noname" $4
    else
        run-bench $1 $2 $3 "put" "noname" $4
    fi
    TABLE=$(readlink -f tests/current)/my_filetable
    echo $TABLE
    run-bench $1 $2 $3 "get" $TABLE $4
    echo $TABLE
    run-bench $1 $2 $3 "delete" $TABLE $4
    # Should we check result here?
}

function run-macro-suite {
    info-print "Starting macro benchmark suite on backend $BACKEND"
    # The duration should be large since we want to complete all
    # operations
    DUR=1000
    for conn in "${CONCURRENT[@]}"; do
        run-bench-cycle $DUR $conn 1 "macro"
    done
}

function run-micro-suite {
    info-print "Starting micro benchmark suite on backend $BACKEND"
    for v_size in "${VAL_SIZE[@]}"; do
        for conn in "${CONCURRENT[@]}"; do
            run-bench-cycle $DURATION $conn $v_size "micro"
        done
    done
}


if [ $SUITE == "macro" ]; then
    run-macro-suite
elif [ $SUITE == "micro" ]; then
    run-micro-suite
fi
