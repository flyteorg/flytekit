
from azure.datalake.store import core, multithread, lib
import logging
def main():
    token = lib.auth()
    print(token)
    aclspec ="user::rwx,group::rwx,other::---,"
    adl = core.AzureDLFileSystem(token, store_name="akharitadls")
        
    adls_logger = logging.getLogger('azure.datalake.store')
    adls_logger.addHandler(logging.FileHandler(filename="adls1.log"))
    adls_logger.setLevel(logging.DEBUG)
    adl.set_acl("/a", aclspec, recursive=True, number_of_sub_process=2)

if __name__ == "__main__":
    main()