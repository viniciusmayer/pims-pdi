package com.eleonorvinicius.pims.pdi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConfiguracaoDAO {

	private static ConfiguracaoDAO configuracaoDAO;
	private static Connection connection; 
	
	private ConfiguracaoDAO() throws SQLException{
		connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/pims", "pims", "viniciusmayer");
	}
	
	public static ConfiguracaoDAO getInstance() throws SQLException{
		if (configuracaoDAO == null){
			configuracaoDAO = new ConfiguracaoDAO();
		}
		return configuracaoDAO;
	}
	
	public String getValor(String chave) throws SQLException {
		String valor = null;

		PreparedStatement preparedStatement = connection.prepareStatement("select c.valor from backend_configuracao c where c.chave = ?");
		preparedStatement.setString(1, chave);
		ResultSet resultSet = preparedStatement.executeQuery();
		while (resultSet.next()) {
			valor = resultSet.getString("valor");
		}
		resultSet.close();
		preparedStatement.close();

		return valor;
	}

}
