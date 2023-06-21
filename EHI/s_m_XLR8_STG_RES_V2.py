

Template file not found for component type COMMAND.

Template file not found for component type SESSION.

Template file not found for component type COMMAND.

Template file not found for component type SESSION.

Template file not found for component type SESSION.


########### Flow definition ###########


COMMAND_CREATE_STG_RES_ODY << s_m_RES_REJECTS_TO_ODYRPT

s_m_DWH_DIM_SOURCE_FACTRES << s_m_XLR8_STG_RES_V2

s_m_RES_REJECTS_TO_ODYRPT << CMD_CREATE_RES2SELECT

s_m_XLR8_STG_RES_V2 << COMMAND_CREATE_STG_RES_ODY