$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force shared\spark-dist | Out-Null
New-Item -ItemType Directory -Force shared\spark-jars | Out-Null

function Download-IfMissing {
  param (
    [Parameter(Mandatory = $true)][string]$Uri,
    [Parameter(Mandatory = $true)][string]$OutFile
  )

  if (Test-Path $OutFile) {
    Write-Host "Exists: $OutFile"
    return
  }

  Write-Host "Downloading: $OutFile"
  Invoke-WebRequest -Uri $Uri -OutFile $OutFile
}

Download-IfMissing `
  -Uri "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz" `
  -OutFile "shared\spark-dist\spark-3.5.1-bin-hadoop3.tgz"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" `
  -OutFile "shared\spark-jars\hadoop-aws-3.3.4.jar"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" `
  -OutFile "shared\spark-jars\aws-java-sdk-bundle-1.12.262.jar"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/13.4.0.jre11/mssql-jdbc-13.4.0.jre11.jar" `
  -OutFile "shared\spark-jars\mssql-jdbc-13.4.0.jre11.jar"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" `
  -OutFile "shared\spark-jars\postgresql-42.7.4.jar"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.0.0/mysql-connector-j-9.0.0.jar" `
  -OutFile "shared\spark-jars\mysql-connector-j-9.0.0.jar"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" `
  -OutFile "shared\spark-jars\delta-spark_2.12-3.2.0.jar"

Download-IfMissing `
  -Uri "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" `
  -OutFile "shared\spark-jars\delta-storage-3.2.0.jar"

