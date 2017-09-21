package oracle.demo.oow.bd.hbase.dao;

import oracle.demo.oow.bd.to.CustomerTO;

public class UserTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		CustomerDAO customerDao = new CustomerDAO();
		CustomerTO customerTO = customerDao.getCustomerByCredential("gue2", "welc");
	}

}
