//! Subscription.

use std::ptr;
use std::{os::raw::c_void, sync::Arc};

use crate::{chkerr, connection::Conn, Connection, DpiSubscr, Result};
use crate::{Context, DpiStmt, OdpiStr};
use odpic_sys::{
    dpiConn_subscribe, dpiSubscr, dpiSubscrMessage, dpiSubscrNamespace, dpiSubscrProtocol,
    dpiSubscrQOS, dpiSubscr_addRef, dpiSubscr_prepareStmt, dpiSubscr_release,
    DPI_SUBSCR_NAMESPACE_AQ, DPI_SUBSCR_NAMESPACE_DBCHANGE, DPI_SUBSCR_PROTO_CALLBACK,
    DPI_SUBSCR_PROTO_HTTP, DPI_SUBSCR_PROTO_MAIL, DPI_SUBSCR_PROTO_PLSQL,
    DPI_SUBSCR_QOS_BEST_EFFORT, DPI_SUBSCR_QOS_DEREG_NFY, DPI_SUBSCR_QOS_QUERY,
    DPI_SUBSCR_QOS_RELIABLE, DPI_SUBSCR_QOS_ROWIDS, DPI_SUCCESS,
};

#[derive(Debug, Default)]
pub enum SubscrNamespace {
    Aq,
    #[default]
    DbChange,
}

impl SubscrNamespace {
    pub(crate) fn to_dpi(self) -> dpiSubscrNamespace {
        match self {
            SubscrNamespace::Aq => DPI_SUBSCR_NAMESPACE_AQ,
            SubscrNamespace::DbChange => DPI_SUBSCR_NAMESPACE_DBCHANGE,
        }
    }
}

#[derive(Debug, Default)]
pub enum SubscrProtocol {
    #[default]
    Callback,
    Http,
    Mail,
    PlSql,
}

impl SubscrProtocol {
    pub(crate) fn to_dpi(self) -> dpiSubscrProtocol {
        match self {
            SubscrProtocol::Callback => DPI_SUBSCR_PROTO_CALLBACK,
            SubscrProtocol::Http => DPI_SUBSCR_PROTO_HTTP,
            SubscrProtocol::Mail => DPI_SUBSCR_PROTO_MAIL,
            SubscrProtocol::PlSql => DPI_SUBSCR_PROTO_PLSQL,
        }
    }
}

pub enum SubscrQos {
    BestEffort,
    DeregNfy,
    Query,
    Reliable,
    Rowids,
}

impl SubscrQos {
    pub(crate) fn to_dpi(self) -> dpiSubscrQOS {
        match self {
            SubscrQos::BestEffort => DPI_SUBSCR_QOS_BEST_EFFORT,
            SubscrQos::DeregNfy => DPI_SUBSCR_QOS_DEREG_NFY,
            SubscrQos::Query => DPI_SUBSCR_QOS_QUERY,
            SubscrQos::Reliable => DPI_SUBSCR_QOS_RELIABLE,
            SubscrQos::Rowids => DPI_SUBSCR_QOS_ROWIDS,
        }
    }
}

pub struct SubscrCreateParams {
    pub namespace: Option<SubscrNamespace>,
    pub protocol: Option<SubscrProtocol>,
    pub qos: Option<SubscrQos>,
    pub operations: Option<u32>,
    pub port_number: Option<u32>,
    pub timeout: Option<u32>,
    pub name: Option<String>,
    pub callback: Option<HandlerWrapper>,
    pub recipient_name: Option<String>,
    pub ip_address: Option<String>,
    pub client_initiated: Option<i32>,
}

impl SubscrCreateParams {
    pub extern "C" fn notification_callback(context: *mut c_void, message: *mut dpiSubscrMessage) {
        unsafe {
            println!("in unsafe notif callback");
            let wrapper_ptr = context as *mut HandlerWrapper;
            let handler = &(*wrapper_ptr).0;
            let msg = NotificationMessage { inner: *message };
            handler(msg);
        }
    }
}

pub struct NotificationMessage {
    pub inner: dpiSubscrMessage,
}

pub struct Subscr {
    pub(crate) conn: Conn,
    pub(crate) handle: DpiSubscr,
}

pub struct HandlerWrapper(pub Box<dyn Fn(NotificationMessage)>);

impl Connection {
    pub fn subscribe(&self, subscr_create_params: SubscrCreateParams) -> Result<Subscr> {
        let ctxt = self.ctxt();
        let mut params = ctxt.subscr_create_params();
        if let Some(namespace) = subscr_create_params.namespace {
            params.subscrNamespace = namespace.to_dpi();
        }
        if let Some(protocol) = subscr_create_params.protocol {
            params.protocol = protocol.to_dpi();
        }
        if let Some(qos) = subscr_create_params.qos {
            params.qos = qos.to_dpi();
        }
        if let Some(operations) = subscr_create_params.operations {
            params.operations = operations;
        }
        if let Some(port_number) = subscr_create_params.port_number {
            params.portNumber = port_number;
        }
        if let Some(timeout) = subscr_create_params.timeout {
            params.timeout = timeout;
        }
        if let Some(name) = subscr_create_params.name {
            let name = OdpiStr::new(name.as_str());
            params.name = name.ptr;
            params.nameLength = name.len;
        }
        if let Some(callback) = subscr_create_params.callback {
            params.callback = Some(SubscrCreateParams::notification_callback);
            params.callbackContext = Box::into_raw(Box::new(callback)) as *mut c_void;
        }
        if let Some(recipient_name) = subscr_create_params.recipient_name {
            let recipient_name = OdpiStr::new(recipient_name.as_str());
            params.recipientName = recipient_name.ptr;
            params.recipientNameLength = recipient_name.len;
        }
        if let Some(ip_address) = subscr_create_params.ip_address {
            let ip_address = OdpiStr::new(ip_address.as_str());
            params.ipAddress = ip_address.ptr;
            params.ipAddressLength = ip_address.len;
        }
        if let Some(client_initiated) = subscr_create_params.client_initiated {
            params.clientInitiated = client_initiated;
        }

        let mut handle = ptr::null_mut();

        chkerr!(
            ctxt,
            dpiConn_subscribe(self.handle(), &mut params, &mut handle)
        );

        Ok(Subscr::from_dpi_handle(self, handle))
    }
}

impl Subscr {
    pub(crate) fn from_dpi_handle(conn: &Connection, handle: *mut dpiSubscr) -> Subscr {
        Subscr {
            conn: Arc::clone(&conn.conn),
            handle: DpiSubscr::new(handle),
        }
    }

    pub(crate) fn ctxt(&self) -> &Context {
        self.conn.ctxt()
    }

    pub(crate) fn handle(&self) -> *mut dpiSubscr {
        self.handle.raw
    }

    pub fn add_ref(&self) -> Result<()> {
        chkerr!(self.ctxt(), dpiSubscr_addRef(self.handle()));

        Ok(())
    }

    pub fn prepare_stmt(&self, sql: String) -> Result<()> {
        let mut handle = DpiStmt::null();
        let sql =OdpiStr::new(sql.as_str());
        println!("{:?}", String::from_utf8(sql.to_string().as_bytes().to_vec()));
        chkerr!(
            self.ctxt(),
            dpiSubscr_prepareStmt(self.handle(), sql.ptr, sql.len, &mut handle.raw)
        );

        Ok(())
    }

    pub fn release(&self, conn: &Connection) -> Result<()> {
        chkerr!(conn.ctxt(), dpiSubscr_release(self.handle()));

        Ok(())
    }
}
