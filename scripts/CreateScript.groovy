b('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.groovy.ScriptedConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.objects.Attribute
import org.identityconnectors.framework.common.objects.AttributesAccessor
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptions
import org.identityconnectors.framework.common.objects.Uid
import org.identityconnectors.framework.common.exceptions.ConnectorException

import java.nio.channels.FileLock
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException

def operation = operation as OperationType
def configuration = configuration as ScriptedConfiguration

def objectClass = objectClass as ObjectClass
def options = options as OperationOptions
def createAttributes = new AttributesAccessor(attributes as Set<Attribute>)
def uid = uid as Uid
def log = log as Log

switch (operation) {
    case OperationType.CREATE:
        println "Entering update script for " + objectClass
        switch (objectClass) {
            case ObjectClass.ACCOUNT:
                def fileLocation = configuration.propertyBag.__ACCOUNT__.fileloc
                def header = ['User_Name', 'User_First_Name', 'User_Last_Name', 'Groups']
                def userName = null
                def firstName = null
                def lastName = null
                def groups = []

                if (updateAttributes.hasAttribute("userName")) {
                    userName = updateAttributes.findString("userName")
                }

                if (updateAttributes.hasAttribute("firstName")) {
                    firstName = updateAttributes.findString("firstName")
                }

                if (updateAttributes.hasAttribute("lastName")) {
                    lastName = updateAttributes.findString("lastName")
                }

                if (updateAttributes.hasAttribute("groups")) {
                    groups = updateAttributes.findStringList("groups")
                }

                if(checkIfRecordExists(fileLocation, userName)){
                    def result = WriteFileWithLock(fileLocation, userName, firstName, lastName, groups)
                } else {
                        throw new ConnectorException("User exists: " + userName)
                }
       }
        return new Uid(userName)
}

def checkIfRecordExists (String fileName, String userName) {
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
    for (line in newData) {
        if (line.User_Name.equalsIgnoreCase(userName)) {
            return true
        }
    }
    return false
}

def WriteFileWithLock(String fileName, String userName, String firstName, String lastName, ArrayList groups) {
    File srcFile = new File (fileName)
    if (!srcFile.exists()) {
        throw new ConnectorException("File not found: " + fileName)
    }
    //println alData
    def bakFileName = fileName + "_bak."+System.currentTimeMillis()
    def bakFile = new File(bakFileName)
    srcFile.withInputStream { input ->
        bakFile.withOutputStream { output ->
            output << input
        }
    }

    // open the file with a RandomAccessFile
    FileChannel channel = new RandomAccessFile(srcFile, 'rw').channel
    // try to get an exclusive lock on this channel
    FileLock lock = null
    while (true) {
        try {
            lock = channel.tryLock()
            break
        } catch (OverlappingFileLockException e) {
            // File is open somewhere else and locked, sleep for a bit and try again
            sleep(1000)
        }
    }
    srcFile.append("\n" + userName + "," + firstName + "," + lastName + "," + groups.join(","))
    lock.release()
    channel.close()
    return true

}