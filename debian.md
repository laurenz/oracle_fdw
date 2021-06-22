Debian/Ubuntu oracle_fdw compile guide
======================================

To compile oracle_fdw for Debian or Ubuntu the Oracle header files
and libraries has to be installed manually.

1. [Installation Requirements](#1-installation-requirements)
2. [Installation](#2-installation)

1 Installation Requirements
===========================

Install the required Debian/Ubuntu packages:

    sudo apt install unzip gcc make postgresql-server-dev-12 libaio1

To obtain the required files go to [Oracle Instant Client Downloads for Linux x86-64 (64-bit)][fd]
and download the following zip files:

 [fd]: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

 * [oracle_fdw][f0] latest tar-ball
 * [Basic Light Package (ZIP)][f1] instantclient-basiclite-linux.x64-21.1.0.0.0.zip
 * [SDK Package (ZIP)][f2] instantclient-sdk-linux.x64-21.1.0.0.0.zip

 [f0]: https://github.com/laurenz/oracle_fdw/releases
 [f1]: https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basiclite-linux.x64-21.1.0.0.0.zip
 [f2]: https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-sdk-linux.x64-21.1.0.0.0.zip

1.1 Library files
-----------------

The library files like `libclntsh.so` are nedded during compile time and runtime.

Unzip the Basic Light zip file with the libraries:

    unzip instantclient-basiclite-linux.x64-21.1.0.0.0.zip

The files will be unzipped to `instantclient_21_1`.
Move the lib-files to `/usr/lib/oracle/21/client/lib`:

    sudo mv instantclient_21_1 /usr/lib/oracle/21/client/lib

Add the new library to the library collection:

    echo /usr/lib/oracle/21/client/lib | sudo tee -a /etc/ld.so.conf.d/oracle-21.conf
    sudo ldconfig /etc/ld.so.conf.d/oracle-21.conf

1.2 Header files
----------------

The header files are only needed during compile time.

Unzip the SDK zip file into 'instantclient_21_1': 

    unzip instantclient-sdk-linux.x64-21.1.0.0.0.zip

Take a note of where the `oci.h` file is located with full path:

    dirname $(readlink -f $(find . -name oci.h))

Sample output:

    /home/joe/instantclient_21_1/sdk/include

1.3 Oracle FDW
--------------

Untar the Oracle FDW and add the path to `oci.h` file in the `Makefile`:

    tar xf oracle_fdw-ORACLE_FDW_2_3_0.tar.gz
    sed -i -e 's|^PG_CPPFLAGS = |&-I/home/joe/instantclient_21_1/sdk/include |' oracle_fdw-ORACLE_FDW_2_3_0/Makefile

Add the new library filder `/usr/lib/oracle/21/client/lib` to the `Makefile`:

    sed -i -e 's|^SHLIB_LINK = |&-L/usr/lib/oracle/21/client/lib |' oracle_fdw-ORACLE_FDW_2_3_0/Makefile

Compile oracle_fdw and install:

    cd oracle_fdw-ORACLE_FDW_2_3_0
    make
    sudo make install

    
2 Installation
==============

Follow the general installation guide for oracle_fdw.

3 Problems
----------

When the oracle_fdw.so has been compiled the path to the libraries can be checked:

    ldd oracle_fdw.so

Sample output:

    libclntsh.so.21.1 => /usr/lib/oracle/21/client/lib/libclntsh.so.21.1 (0x00007f62f3cb3000)
    libaio.so.1 => /lib/x86_64-linux-gnu/libaio.so.1 (0x00007f62f318d000)
    libclntshcore.so.21.1 => /usr/lib/oracle/21/client/lib/libclntshcore.so.21.1 (0x00007f62f2bc0000)
    ...
