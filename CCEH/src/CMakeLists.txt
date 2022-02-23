# CMAKELISTS.TXT UNDER cceh
set(CCEH_SRCS
    "CCEH_buflog.cpp"
    "CCEH_buflog.h"
    "CCEH_buflog_limit.cpp"
    "CCEH_buflog_limit.h"
    "CCEH_BUF_Exchange.cpp"
    "CCEH_BUF_Exchange.h"
    "CCEH_BUF_dual.cpp"
    "CCEH_BUF_dual.h"
    "CCEH_buflog_recovery.cpp"
    "CCEH_buflog_recovery.h"
    "CCEH_buflog_wal.cpp"
    "CCEH_buflog_wal.h"
    "trace.cc"
    "trace.h"
)

# Declare the library
add_library(cceh STATIC
    ${CCEH_SRCS}
)


# add_dependencies(cceh)

# Specify here the include directories exported by this library
target_include_directories(cceh PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)