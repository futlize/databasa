package adminapi

const (
	ServiceName = "databasa.Admin"

	MethodLogin             = "Login"
	MethodCreateUser        = "CreateUser"
	MethodAlterUserPassword = "AlterUserPassword"
	MethodDropUser          = "DropUser"
	MethodListUsers         = "ListUsers"
	MethodBootstrapStatus   = "BootstrapStatus"
)

const (
	FullMethodLogin             = "/" + ServiceName + "/" + MethodLogin
	FullMethodCreateUser        = "/" + ServiceName + "/" + MethodCreateUser
	FullMethodAlterUserPassword = "/" + ServiceName + "/" + MethodAlterUserPassword
	FullMethodDropUser          = "/" + ServiceName + "/" + MethodDropUser
	FullMethodListUsers         = "/" + ServiceName + "/" + MethodListUsers
	FullMethodBootstrapStatus   = "/" + ServiceName + "/" + MethodBootstrapStatus
)
