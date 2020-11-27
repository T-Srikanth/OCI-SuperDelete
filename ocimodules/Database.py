import oci
import time

WaitRefresh = 10

def DeleteDBCS(config, Compartments):
    AllItems = []
    object = oci.database.DatabaseClient(config)

    print ("Getting all Database objects, might take a little longer based on number of DB systems present")
    for Compartment in Compartments:
        retry = True
        while retry:
            retry = False
            try:
                items = oci.pagination.list_call_get_all_results(object.list_db_systems, compartment_id=Compartment.id).data
                for item in items:
                    if (item.lifecycle_state != "TERMINATED"):
                        AllItems.append(item)
                        print("- {} - {}".format(item.display_name, item.lifecycle_state))
            except Exception as e:
                if e.status == 429:
                    print ("Delaying.. api calls")
                    time.sleep(10)
                    retry = True                         

    itemsPresent = True

    while itemsPresent:
        count = 0
        for item in AllItems:
            retry = True
            while retry:
                retry = False
                try:
                    itemstatus = object.get_db_system(db_system_id=item.id).data
                    if itemstatus.lifecycle_state != "TERMINATED":
                        if itemstatus.lifecycle_state != "TERMINATING":
                            try:
                                print ("Deleting: {}".format(itemstatus.display_name))
                                object.terminate_db_system(db_system_id=itemstatus.id)
                            except:
                                print ("error trying to delete: {}".format(itemstatus.display_name))
                        else:
                            print("{} = {}".format(itemstatus.display_name, itemstatus.lifecycle_state))
                        count = count + 1
                except Exception as e:
                    if e.status == 429:
                        print ("Delaying.. api calls")
                        time.sleep(10)
                        retry = True                
                    else:
                        print ("error getting : {}".format(item.display_name))
                        retry = False
        if count > 0 :
            print ("Waiting for all Objects to be deleted...")
            time.sleep(40)
        else:
            itemsPresent = False
    print ("All Objects deleted!")           

def DeleteAutonomousDB(config, Compartments):
    AllItems = []
    object = oci.database.DatabaseClient(config)

    print ("Getting all Autonomous Database objects")
    for Compartment in Compartments:
        items = oci.pagination.list_call_get_all_results(object.list_autonomous_databases, compartment_id=Compartment.id).data
        for item in items:
            if (item.lifecycle_state != "TERMINATED"):
                AllItems.append(item)
                print("- {} - {}".format(item.display_name, item.lifecycle_state))

    itemsPresent = True

    while itemsPresent:
        count = 0
        for item in AllItems:
            try:
                itemstatus = object.get_autonomous_database(autonomous_database_id=item.id).data
                if itemstatus.lifecycle_state != "TERMINATED":
                    if itemstatus.lifecycle_state != "TERMINATING":
                        try:
                            print ("Deleting: {}".format(itemstatus.display_name))
                            object.delete_autonomous_database(autonomous_database_id=itemstatus.id)
                        except:
                            print ("error trying to delete: {}".format(itemstatus.display_name))
                    else:
                        print("{} = {}".format(itemstatus.display_name, itemstatus.lifecycle_state))
                    count = count + 1
            except:
                print ("error getting : {}".format(item.display_name))
        if count > 0 :
            print ("Waiting for all Objects to be deleted...")
            time.sleep(WaitRefresh)
        else:
            itemsPresent = False
    print ("All Objects deleted!")


def DeleteDBBackups(config, Compartments):
    AllItems = []
    object = oci.database.DatabaseClient(config)

    print ("Getting all Database Backup objects")
    for Compartment in Compartments:
        items = oci.pagination.list_call_get_all_results(object.list_backups, compartment_id=Compartment.id).data
        for item in items:
            if (item.lifecycle_state != "DELETED"):
                AllItems.append(item)
                print("- {} - {}".format(item.display_name, item.lifecycle_state))

    itemsPresent = True

    while itemsPresent:
        count = 0
        for item in AllItems:
            try:
                itemstatus = object.get_backup(backup_id=item.id).data
                if itemstatus.lifecycle_state != "DELETED":
                    if itemstatus.lifecycle_state != "DELETING":
                        try:
                            print ("Deleting: {}".format(itemstatus.display_name))
                            object.delete_backup(backup_id=itemstatus.id)
                        except:
                            print ("error trying to delete: {}".format(itemstatus.display_name))
                    else:
                        print("{} = {}".format(itemstatus.display_name, itemstatus.lifecycle_state))
                    count = count + 1
            except:
                print ("error getting : {}".format(item.display_name))
        if count > 0 :
            print ("Waiting for all Objects to be deleted...")
            time.sleep(WaitRefresh)
        else:
            itemsPresent = False
    print ("All Objects deleted!")

def DeleteAutonomousContainerDatabase(config, Compartments):
    AllItems = []
    object = oci.database.DatabaseClient(config)

    print ("Getting all Autonomous Container Database objects")
    for Compartment in Compartments:
        items = oci.pagination.list_call_get_all_results(object.list_autonomous_container_databases, compartment_id=Compartment.id).data
        for item in items:
            if (item.lifecycle_state != "TERMINATED"):
                AllItems.append(item)
                print("- {} - {}".format(item.display_name, item.lifecycle_state))

    itemsPresent = True

    while itemsPresent:
        count = 0
        for item in AllItems:
            try:
                itemstatus = object.get_autonomous_container_database(autonomous_container_database_id=item.id).data
                if itemstatus.lifecycle_state != "TERMINATED":
                    if itemstatus.lifecycle_state != "TERMINATING":
                        try:
                            print ("Deleting: {}".format(itemstatus.display_name))
                            object.terminate_autonomous_container_database(autonomous_container_database_id=itemstatus.id)
                        except:
                            print ("error trying to delete: {}".format(itemstatus.display_name))
                    else:
                        print("{} = {}".format(itemstatus.display_name, itemstatus.lifecycle_state))
                    count = count + 1
            except:
                print ("error getting : {}".format(item.display_name))
        if count > 0 :
            print ("Waiting for all Objects to be deleted...")
            time.sleep(60)
        else:
            itemsPresent = False
    print ("All Objects deleted!")

def DeleteAutonomousExadataInfrastructure(config, Compartments):
    AllItems = []
    object = oci.database.DatabaseClient(config)

    print ("Getting all Autonomous Exadata Infrastructure objects")
    for Compartment in Compartments:
        items = oci.pagination.list_call_get_all_results(object.list_autonomous_exadata_infrastructures, compartment_id=Compartment.id).data
        for item in items:
            if (item.lifecycle_state != "TERMINATED"):
                AllItems.append(item)
                print("- {} - {}".format(item.display_name, item.lifecycle_state))

    itemsPresent = True

    while itemsPresent:
        count = 0
        for item in AllItems:
            try:
                itemstatus = object.get_autonomous_exadata_infrastructure(autonomous_exadata_infrastructure_id=item.id).data
                if itemstatus.lifecycle_state != "TERMINATED":
                    if itemstatus.lifecycle_state != "TERMINATING":
                        try:
                            print ("Deleting: {}".format(itemstatus.display_name))
                            object.terminate_autonomous_exadata_infrastructure(autonomous_exadata_infrastructure_id=itemstatus.id)
                        except:
                            print ("error trying to delete: {}".format(itemstatus.display_name))
                    else:
                        print("{} = {}".format(itemstatus.display_name, itemstatus.lifecycle_state))
                    count = count + 1
            except:
                print ("error getting : {}".format(item.display_name))
        if count > 0 :
            print ("Waiting for all Objects to be deleted...")
            time.sleep(WaitRefresh)
        else:
            itemsPresent = False
    print ("All Objects deleted!")
