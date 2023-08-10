@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.groovy.ScriptedConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptions
import org.forgerock.openicf.connectors.groovy.MapFilterVisitor
import org.identityconnectors.framework.common.objects.filter.Filter
import org.identityconnectors.framework.common.objects.filter.EqualsFilter
import org.identityconnectors.framework.common.objects.filter.OrFilter
import org.identityconnectors.framework.common.objects.filter.FilterBuilder
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter
import org.identityconnectors.framework.common.exceptions.ConnectorException
import org.identityconnectors.framework.common.FrameworkUtil


def operation = operation as OperationType
def configuration = configuration as ScriptedConfiguration
def filter = filter as Filter
def log = log as Log
def objectClass = objectClass as ObjectClass
def options = options as OperationOptions

//println "########## Entering " + operation + " Script"
//println "########## ObjectClass: " + objectClass.objectClassValue
def map = [:]
//def queryFilter = 'true'


switch (objectClass) {
    case objectClass.ACCOUNT:
        // Handle the results

        def fileLocation = null
        //fileLocation = configuration.propertyBag.__ACCOUNT__.fileloc
        //Hardcoding because of the issue with the configuration where stale values are being used
        fileLocation = '/Users/sanjay.rallapally/Downloads/tungsten/ap-tungsten-accounts.csv'
        def resources = null
        if(null != fileLocation) {
            println " Account: Loading " + fileLocation
            resources = loadAccountDatav2(fileLocation)
        } else {
            throw new ConnectorException("Account: File location not specified")
        }
        String pagedResultsCookie = options.getPagedResultsCookie();
        String currentPagedResultsCookie = options.getPagedResultsCookie();
        if(filter != null){
            def username = null
            if (filter instanceof EqualsFilter){
                def attrName = ((EqualsFilter) filter).getAttribute()
                if (attrName.is(Uid.NAME) || attrName.is(Name.NAME)) {
                    username = ((EqualsFilter) filter).getAttribute().getValue().get(0)
                }

                //def uuid = FrameworkUtil.getUidIfGetOperation(filter)
                //println "Searching for " + uuid.uidValue
                if(username != null){
                    //println "UID " + uuid.uidValue
                    //resource = resources.find { row ->
                    //    row.User_Name.toUpperCase() == uuid.uidValue.toUpperCase()
                    //}
                    resource = resources.find { row ->
                        row.User_Name.toUpperCase() == username.toUpperCase()
                    }
                    if(resource != null && !"".equals(resource)) {
                        def groupList = Arrays.asList(resource.Groups.split("\\s*,\\s*"));
                        handler {
                            uid resource.User_Name
                            id  resource.User_Name
                            attribute 'lastName', resource.User_Last_Name
                            attribute 'firstName', resource.User_First_Name
                            attribute 'userName', resource.User_Name
                            attribute 'groups', groupList
                        }
                    }
                }
            } else if(filter instanceof OrFilter){
                def keys = getOrFilters((OrFilter)filter)
                println "OR Filter: " + keys
                def OrResource = []
                resources.each {
                    if(keys.contains(it.User_Name)){
                        OrResource.add(it)
                    }
                }
                OrResource.each { row ->
                    def grpList = Arrays.asList(row.Groups.split("\\s*,\\s*"));
                    handler {
                        uid row.User_Name
                        id  row.User_Name
                        attribute 'lastName', row.User_Last_Name
                        attribute 'firstName', row.User_First_Name
                        attribute 'userName', row.User_Name
                        attribute 'groups', grpList
                    }
                }
                //println "Filter to type " + filter.getClass().getName() + " not supported"
            }
            return new SearchResult()
        } else {
            if (null != options.getPageSize()) {
                Integer pagedResultsOffset =
                        null != options.getPagedResultsOffset() ? Math.max(0, options
                                .getPagedResultsOffset()) : 0;
                final Integer pageSize = options.getPageSize();
                if(options.pagedResultsCookie) {
                    lastHandledIndex = resources.findIndexOf { resource ->
                        resource.uid == new String(options.pagedResultsCookie.decodeBase64Url())
                    }
                } else if (options.pagedResultsOffset){
                    resources = resources.drop options.pagedResultsOffset
                }
                def remainingPagedResults = resources.size() - pageSize
                resources = resources.subList 0, Math.min(pageSize, resources.size())
            } else {
                println "################ No paging for Account"
            }
            resources.each { row ->
                def groupList = Arrays.asList(row.Groups.split("\\s*,\\s*"));
                handler {
                    uid row.User_Name
                    id  row.User_Name
                    attribute 'lastName', row.User_Last_Name
                    attribute 'firstName', row.User_First_Name
                    attribute 'userName', row.User_Name
                    attribute 'groups', groupList
                }
            }
            return new SearchResult(pagedResultsCookie,-1);
        }
        break
    case objectClass.GROUP:
        def grpfileLocation = null
        grpfileLocation  = '/Users/sanjay.rallapally/Downloads/tungsten/ap-tungsten-groups.csv'
        //configuration.propertyBag.__GROUP__.fileloc
        def resources = null
        def grpName = null
        if(null != grpfileLocation) {
            println "Group: Loading " + grpfileLocation
            resources = loadGroupData(grpfileLocation)
        } else {
            throw new ConnectorException("Group: File location not specified")
        }
        String pagedResultsCookie = options.getPagedResultsCookie();
        String currentPagedResultsCookie = options.getPagedResultsCookie();
        if(filter != null){
            if (filter instanceof EqualsFilter){
                def attrName = ((EqualsFilter) filter).getAttribute()
                if (attrName.is(Uid.NAME) || attrName.is(Name.NAME)) {
                    grpName = ((EqualsFilter) filter).getAttribute().getValue().get(0)
                }
                // println "Group Name " + grpName
                //println "######################### GROUP EQUALS FILTER##############"
            } else if (filter instanceof OrFilter){
                println "######################### GROUP OR FILTER##############"
            }
            println "Here is the Group filter: " + filter
            //def uuid = FrameworkUtil.getUidIfGetOperation(filter)
            //println "Searching " + objectClass.objectClassValue + " for " + uuid
            //if(uuid){
            if(grpName != null){
                resource = resources.find { row ->
                    //row.GROUP_NAME == uuid.uidValue
                    row.GROUP_NAME == grpName.toUpperCase()
                }
                if(resource.size() == 0){
                    throw new ConnectorException("No user found with uid: " + uuid)
                } else {
                    handler {
                        uid resource.GROUP_NAME
                        id  resource.GROUP_NAME
                        attribute 'groupName', resource.GROUP_NAME
                        attribute 'groupDisplayName', resource.GROUP_DESC
                    }
                }
            }
            return new SearchResult(pagedResultsCookie,-1);
        } else {
            if (null != options.getPageSize()) {

                Integer pagedResultsOffset =
                        null != options.getPagedResultsOffset() ? Math.max(0, options
                                .getPagedResultsOffset()) : 0;
                final Integer pageSize = options.getPageSize();
                if(options.pagedResultsCookie) {
                    lastHandledIndex = resources.findIndexOf { resource ->
                        resource.uid == new String(options.pagedResultsCookie.decodeBase64Url())
                    }
                } else if (options.pagedResultsOffset){
                    resources = resources.drop options.pagedResultsOffset
                }
                def remainingPagedResults = resources.size() - pageSize
                resources = resources.subList 0, Math.min(pageSize, resources.size())
                //println "################ Paging for Group " + resources.size()
            } else {
                println "################ No paging for Group " + resources.size()
            }
            resources.each { row ->
                handler {
                    uid row.GROUP_NAME
                    id  row.GROUP_NAME
                    attribute 'groupName', row.GROUP_NAME
                    attribute 'groupDisplayName', row.GROUP_DESC
                }
            }
            return new SearchResult(pagedResultsCookie,-1);

        }
        break
    default:
        break
}
def loadAccountDatav2 (String fileName) {
    def accountFile = null
    accountFile = new File (fileName)
    if (!accountFile.exists()) {
        throw new ConnectorException("Account: File not found: " + fileName)
    }
    def csvContent = accountFile.text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def newData = csvData.collect { row ->
        [User_Name: row.User_Name, User_First_Name: row.User_First_Name, User_Last_Name: row.User_Last_Name, Groups: row.Groups]
    }
    // Sort by 'User_Name'.
    newData.sort { a, b -> a.User_Name <=> b.User_Name }
    //println newData
    return newData

}

def loadGroupData (String fileName) {
    File csvFile = new File (fileName)
    if (!csvFile.exists()) {
        throw new ConnectorException("File not found: " + fileName)
    }
    def csvContent = csvFile.text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def newData = csvData.collect { row ->
        [GROUP_NAME: row.GROUP_NAME, GROUP_DESC: row.GROUP_DESC]
    }

    // Eliminate duplicates from newData using GROUP_NAME as the key
    def uniqueGroupData = newData.unique().sort { a, b -> a.GROUP_NAME <=> b.GROUP_NAME }
    //println uniqueGroupData
    return uniqueGroupData
}

def getOrFilters(OrFilter filter) {
    def ids = []
    Filter left = filter.getLeft()
    Filter right = filter.getRight()
    if(left instanceof EqualsFilter) {
        String id = ((EqualsFilter)left).getAttribute().getValue().get(0).toString()
        ids.add(id)
    } else if(left instanceof OrFilter) {
        ids.addAll(getOrFilters((OrFilter)left))
    }
    if(right instanceof EqualsFilter) {
        String id = ((EqualsFilter)right).getAttribute().getValue().get(0).toString()
        ids.add(id)
    } else if(right instanceof OrFilter) {
        ids.addAll(getOrFilters((OrFilter)right))
    }
    return ids

}