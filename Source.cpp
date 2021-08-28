#include <iostream>
#include <sstream>
#include <fstream>
#include <memory>
#include <iomanip>
#include <string>
#include <stdexcept>
#include "mysqlx/xdevapi.h"

//#pragma comment(lib, "libmysql")

//#pragma comment(lib, "mysqlclient")

using namespace mysqlx;
using namespace std;
// Scope controls life-time of objects such as session or schema

int main(){


	try {

		Session sess("localhost", "root", "Zjae3783@123");
		Schema dbAccounts = sess.getSchema("account_info");
		// or Schema db(sess, "test");

		sess.sql("USE account_info").execute();

		sess.sql("DROP TABLE IF EXISTS dest").execute();		
		sess.sql("DROP PROCEDURE IF EXISTS get_suspicious_transactions;").execute();
		
		sess.sql("CREATE PROCEDURE get_suspicious_transactions() "
			"BEGIN "
			"  CREATE TABLE dest AS (SELECT T.account_number, T.merchant_description, T.transaction_number, T.transaction_amount, A.last_name, A.first_name FROM transactions T LEFT JOIN account_info A ON T.account_number = A.account_number); "
			"  SELECT D.account_number, D.merchant_description, D.transaction_number, D.transaction_amount, D.last_name, D.first_name, mean.average, mean.deviation FROM dest AS D LEFT JOIN(SELECT D.account_number, AVG(D.transaction_amount) AS average, STDDEV(D.transaction_amount) AS deviation FROM dest D GROUP BY D.account_number) AS mean ON mean.account_number = D.account_number WHERE D.transaction_amount > mean.average + 7 * mean.deviation; "
			"END;").execute();

		RowResult res = sess.sql("CALL get_suspicious_transactions();").execute();

		sess.sql("DROP PROCEDURE get_suspicious_transactions;").execute();

		std::stringstream output;

		Row rowResult = res.fetchOne();

		col_count_t nColumns = res.getColumnCount();

		int w = 15;

		for (int idx = 0; idx < static_cast<int>(nColumns) - 1; idx += 1) {

			const Column& colName = res.getColumn(idx);

			if (idx < 4)
				output.width(35);
			else
				output.width(15);

			output << std::left << colName.getColumnName() << "\t";
		}

		output << "\n";

		while (rowResult) {

			for ( int idx = 0; idx < static_cast<int>(nColumns) - 1; idx += 1 ) {

				auto val = rowResult.get(idx);

				if (idx < 4)
					output.width(35);
				else
					output.width(15);

				output << std::left << val << "\t";
			}
			output << "\n";
			rowResult = res.fetchOne();
		}

		std::cout << output.str() << std::endl;

		ofstream myFile("filename.txt");
		myFile << output.str();
		// Close the file
		myFile.close();

		output.str(std::string());

		sess.sql("DROP PROCEDURE IF EXISTS get_suspicious_transaction_locations;").execute();
		
		sess.sql("DROP TABLE IF EXISTS dest_loc").execute();

		sess.sql("CREATE PROCEDURE get_suspicious_transaction_locations() "
			"BEGIN "
			"  CREATE TABLE dest_loc AS (SELECT T.account_number, T.merchant_description, T.transaction_number, T.transaction_amount, T.transaction_location AS actual_transaction_location, A.last_name, A.first_name, A.state AS expected_transaction_location FROM transactions T LEFT JOIN account_info A ON T.account_number = A.account_number); "
			"  SELECT D.first_name, D.last_name, D.account_number, D.transaction_number, D.expected_transaction_location, D.actual_transaction_location FROM dest_loc D WHERE strcmp(SUBSTR(D.actual_transaction_location,1,2), SUBSTR(D.expected_transaction_location,1,2)) != 0; "
			"END;").execute();

		res = sess.sql("CALL get_suspicious_transaction_locations();").execute();

		rowResult = res.fetchOne();

		nColumns = res.getColumnCount();

		output << "\n";

		for (int idx = 0; idx < static_cast<int>(nColumns); idx += 1) {

			const Column& colName = res.getColumn(idx);

			if (idx < 3)
				output.width(10);
			else
				output.width(35);

			output << std::left << colName.getColumnName() << "\t";
		}

		output << "\n";

		while (rowResult) {

			for (int idx = 0; idx < static_cast<int>(nColumns); idx += 1) {

				auto val = rowResult.get(idx);

				if (idx < 3)
					output.width(10);
				else
					output.width(35);

				output << std::left << val << "\t";
			}
			output << "\n";
			rowResult = res.fetchOne();
		}

		cout << output.str() << std::endl;

		ofstream myFileLocations("filename_locations.txt");
		myFileLocations << output.str();
		// Close the file
		myFileLocations.close();
		
		output.str(std::string());

		sess.sql("DROP PROCEDURE IF EXISTS drop_tables;").execute();

		sess.sql("CREATE PROCEDURE drop_tables() "
			"BEGIN "
			"	DROP TABLE IF EXISTS transactions; "
			"	DROP TABLES IF EXISTS account_info; "
			"	DROP TABLES IF EXISTS dest; "
			"END;").execute();
		
		//sess.sql("CALL drop_tables();").execute();
		sess.sql("DROP PROCEDURE drop_tables;").execute();
	}
	catch (const Error& err) {
		cout << "The database session could not be opened : " <<  err << endl;
	}

	cout << "Done";

	return 0;
}