package com.eleonorvinicius.pims.pdi;

public enum ConfiguracaoEnum {

	QUEUE_HOST("localhost"),
	QUEUE_NAME("pims"),
	DB_URL("jdbc:postgresql://localhost:5432/pims"),
	DB_USER("pims"),
	DB_PASSWORD("viniciusmayer"),
	PDI_JOB_EXTENSION("kjb");

	private String defaultValue;
	
	private ConfiguracaoEnum(String defaultValue){
		this.defaultValue = defaultValue;
	}

	public String getDefaultValue() {
		return defaultValue;
	}
}