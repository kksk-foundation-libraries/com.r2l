package com.r2l.constants;

public class RdbAction {
	private RdbAction() {
	}

	public static final byte INSERT_OR_UPDATE = 0;
	public static final byte INSERT = INSERT_OR_UPDATE + 1;
	public static final byte UPDATE = INSERT + 1;
	public static final byte DELETE = UPDATE + 1;
}
