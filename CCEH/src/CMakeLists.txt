# CMAKELISTS.TXT UNDER cceh
set(CCEH_SRCS
    # "CCEH_BUF.cpp"
    # "CCEH_BUF.h"
    # "CCEH_buflog.cpp"
    # "CCEH_buflog.h"
    # "CCEH_BUF_Exchange.cpp"
    # "CCEH_BUF_Exchange.h"
    "CCEH_BUF_dual.cpp"
    "CCEH_BUF_dual.h"
    # "CCEH_buflog_dist.cpp"
    # "CCEH_buflog_dist.h"
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