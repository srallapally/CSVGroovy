@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.groovy.ScriptedConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptions
import org.identityconnectors.framework.common.objects.filter.Filter
import org.identityconnectors.framework.common.objects.filter.EqualsFilter
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

println "########## Entering " + operation + " Script"
println "########## ObjectClass: " + objectClass.objectClassValue
def query = [:]
def queryFilter = 'true'


switch (objectClass) {
    case objectClass.ACCOUNT:
        // Handle the results

        def fileLocation = configuration.propertyBag.__ACCOUNT__.fileloc
        def resources = null
        if(null != fileLocation) {
            println "Loading " + fileLocation
            resources = loadAccountData(fileLocation)
        } else {
            throw new ConnectorException("File location not specified")
        }
        String pagedResultsCookie = options.getPagedResultsCookie();
        String currentPagedResultsCookie = options.getPagedResultsCookie();
        if(filter != null){
            def uuid = FrameworkUtil.getUidIfGetOperation(filter)
            if(uuid){
                resource = resources.find { row ->
                    row.User_Name == uuid.uidValue
                }
                if(resource.size() == 0){
                    throw new ConnectorException("No user found with uid: " + uuid)
                } else {
                    handler {
                        uid resource.User_Name
                        id  resource.User_Name
                        attribute 'lastName', resource.User_Last_Name
                        attribute 'firstName', resource.User_First_Name
                        attribute 'userName', resource.User_Name
                        attribute 'groups', resource.Groups
                    }
                }
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
                handler {
                    uid row.User_Name
                    id  row.User_Name
                    attribute 'lastName', row.User_Last_Name
                    attribute 'firstName', row.User_First_Name
                    attribute 'userName', row.User_Name
                    attribute 'groups', row.Groups
                }
            }
            return new SearchResult(pagedResultsCookie,-1);
        }
    case objectClass.GROUP:
        def fileLocation = configuration.propertyBag.__GROUP__.fileloc
        def resources = null
        if(null != fileLocation) {
            println "Loading " + fileLocation
            resources = loadGroupData(fileLocation)
        } else {
            throw new ConnectorException("File location not specified")
        }
        String pagedResultsCookie = options.getPagedResultsCookie();
        String currentPagedResultsCookie = options.getPagedResultsCookie();
        if(filter != null){
            def uuid = FrameworkUtil.getUidIfGetOperation(filter)
            println "Searching " + objectClass.objectClassValue + " for " + uuid
            if(uuid){
                resource = resources.find { row ->
                    row.GROUP_NAME == uuid.uidValue
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
                println "################ Paging for Group " + resources.size()
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
    default:
        break
}

def loadAccountData (String fileName) {
    File csvFile = new File (fileName)
    if (!csvFile.exists()) {
        throw new ConnectorException("File not found: " + fileName)
    }
    def csvContent = csvFile.text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def newData = csvData.collect { row ->
        [User_Name: row.User_Name, User_First_Name: row.User_First_Name, User_Last_Name: row.User_Last_Name, GROUP_NAME: row.GROUP_NAME]
    }
    // Sort by 'User_Name'.
    newData.sort { a, b -> a.User_Name <=> b.User_Name }

    // Group by 'User_Name' and collect 'GROUP_NAME' into 'Groups'.
    def groupedData = newData.groupBy { it.User_Name }.collect { userName, rows ->
        def groups = rows*.GROUP_NAME.join(', ')
        def list = []
        list = groups.split("\\s*,\\s*")
        rows[0] + [Groups: list]
    }
    return groupedData
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

