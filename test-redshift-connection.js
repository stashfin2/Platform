/**
 * Diagnostic script to test Redshift connections
 * Run: node test-redshift-connection.js
 */

require('dotenv').config();
const { RedshiftDataClient, ExecuteStatementCommand, DescribeStatementCommand } = require('@aws-sdk/client-redshift-data');

async function testPrimaryRedshift() {
  console.log('\n=== Testing PRIMARY Redshift ===');
  console.log('Region:', process.env.AWS_REGION);
  console.log('Cluster:', process.env.REDSHIFT_CLUSTER_IDENTIFIER);
  console.log('Database:', process.env.REDSHIFT_DATABASE);
  console.log('User:', process.env.REDSHIFT_DB_USER);
  console.log('Table:', process.env.REDSHIFT_TABLE_NAME);

  try {
    const client = new RedshiftDataClient({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });

    console.log('\n✓ Primary client created');

    const command = new ExecuteStatementCommand({
      ClusterIdentifier: process.env.REDSHIFT_CLUSTER_IDENTIFIER,
      Database: process.env.REDSHIFT_DATABASE,
      DbUser: process.env.REDSHIFT_DB_USER,
      Sql: 'SELECT 1 as test;',
    });

    console.log('Executing test query...');
    const response = await client.send(command);
    console.log('✅ PRIMARY REDSHIFT - SUCCESS!');
    console.log('Statement ID:', response.Id);
    
    // Wait for completion
    await waitForStatement(client, response.Id);
    
    return true;
  } catch (error) {
    console.error('❌ PRIMARY REDSHIFT - FAILED!');
    console.error('Error:', error.message);
    console.error('Error Name:', error.name);
    if (error.$metadata) {
      console.error('HTTP Status:', error.$metadata.httpStatusCode);
    }
    return false;
  }
}

async function testSecondaryRedshift() {
  console.log('\n=== Testing SECONDARY Redshift ===');
  console.log('Enabled:', process.env.SECONDARY_REDSHIFT_ENABLED);
  
  if (process.env.SECONDARY_REDSHIFT_ENABLED !== 'true') {
    console.log('⚠️  Secondary Redshift is disabled');
    return null;
  }

  console.log('Endpoint:', process.env.SECONDARY_REDSHIFT_ENDPOINT);
  console.log('Region:', process.env.SECONDARY_AWS_REGION);
  console.log('Cluster:', process.env.SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER);
  console.log('Database:', process.env.SECONDARY_REDSHIFT_DATABASE);
  console.log('User:', process.env.SECONDARY_REDSHIFT_DB_USER);
  console.log('Table:', process.env.SECONDARY_REDSHIFT_TABLE_NAME);

  try {
    const clientConfig = {
      region: process.env.SECONDARY_AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    };

    if (process.env.SECONDARY_REDSHIFT_ENDPOINT) {
      clientConfig.endpoint = process.env.SECONDARY_REDSHIFT_ENDPOINT;
    }

    const client = new RedshiftDataClient(clientConfig);
    console.log('\n✓ Secondary client created');

    const command = new ExecuteStatementCommand({
      ClusterIdentifier: process.env.SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER,
      Database: process.env.SECONDARY_REDSHIFT_DATABASE,
      DbUser: process.env.SECONDARY_REDSHIFT_DB_USER,
      Sql: 'SELECT 1 as test;',
    });

    console.log('Executing test query...');
    const response = await client.send(command);
    console.log('✅ SECONDARY REDSHIFT - SUCCESS!');
    console.log('Statement ID:', response.Id);
    
    // Wait for completion
    await waitForStatement(client, response.Id);
    
    return true;
  } catch (error) {
    console.error('❌ SECONDARY REDSHIFT - FAILED!');
    console.error('Error:', error.message);
    console.error('Error Name:', error.name);
    if (error.$metadata) {
      console.error('HTTP Status:', error.$metadata.httpStatusCode);
    }
    return false;
  }
}

async function waitForStatement(client, statementId) {
  for (let i = 0; i < 10; i++) {
    const command = new DescribeStatementCommand({ Id: statementId });
    const response = await client.send(command);
    
    if (response.Status === 'FINISHED') {
      console.log('✓ Query completed successfully');
      return;
    } else if (response.Status === 'FAILED' || response.Status === 'ABORTED') {
      console.error('✗ Query failed:', response.Error);
      throw new Error(`Query failed: ${response.Error}`);
    }
    
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

async function main() {
  console.log('='.repeat(60));
  console.log('REDSHIFT CONNECTION DIAGNOSTIC');
  console.log('='.repeat(60));

  const primaryResult = await testPrimaryRedshift();
  const secondaryResult = await testSecondaryRedshift();

  console.log('\n' + '='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log('Primary Redshift:  ', primaryResult ? '✅ SUCCESS' : '❌ FAILED');
  if (secondaryResult !== null) {
    console.log('Secondary Redshift:', secondaryResult ? '✅ SUCCESS' : '❌ FAILED');
  } else {
    console.log('Secondary Redshift: ⚠️  DISABLED');
  }
  console.log('='.repeat(60));
  
  process.exit(primaryResult ? 0 : 1);
}

main();

