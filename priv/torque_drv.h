#include <pbs_ifl.h>
#include <pbs_error.h>
#include <erl_driver.h>
#include <ei.h>
#include <erl_interface.h>

#define CMD_CONNECT           0
#define CMD_STAT_JOB          1
#define CMD_STAT_QUEUE        2
#define CMD_STAT_JOB_A        3
#define CMD_STAT_QUEUE_A      4

#define ATTR_EXECUTION_TIME   1
#define ATTR_ACCOUNT_NAME     2
#define ATTR_CHECKPOINT       3
#define ATTR_ERROR_PATH       4
#define ATTR_GROUP_LIST       5
#define ATTR_HOLD_TYPES       6
#define ATTR_JOIN_PATHS       7
#define ATTR_KEEP_FILES       8
#define ATTR_RESOURCE_LIST    9
#define ATTR_MAIL_POINTS      10
#define ATTR_MAIL_USERS       11
#define ATTR_JOB_NAME         12
#define ATTR_OUTPUT_PATH      13
#define ATTR_PRIORITY         14
#define ATTR_DESTINATION      15
#define ATTR_RERUNABLE        16
#define ATTR_SESSION_ID       17
#define ATTR_SHELL_PATH_LIST  18
#define ATTR_USER_LIST        19
#define ATTR_VARIABLE_LIST    20
#define ATTR_CREATE_TIME      21
#define ATTR_DEPEND           22
#define ATTR_MODIF_TIME       23
#define ATTR_QUEUE_TIME       24
#define ATTR_QUEUE_TYPE       25
#define ATTR_STAGEIN          26
#define ATTR_STAGEOUT         27
#define ATTR_JOB_STATE        28

typedef struct torque_drv_t {
    unsigned int  key;
    ErlDrvPort    port;
    FILE          *log;
    int           pbs_connect;
} torque_drv_t;

static ErlDrvData
start (ErlDrvPort port, 
       char *cmd);

static void
stop (ErlDrvData drv);

static int
control (ErlDrvData drv,
         unsigned int command,
         char *buf,
         int len,
         char **rbuf,
         int rlen);

static void
ready_async (ErlDrvData drv,
             ErlDrvThreadData thread_data);

static int
torque_disconnect (torque_drv_t *drv);

static int
torque_connect (torque_drv_t *drv, char *server);

static int
stat_job (torque_drv_t *drv, 
          char *job_name,
          struct attrl *filter);

static int
stat_queue (torque_drv_t *drv,
            char *queue_name,
            struct attrl *filter);

static int
stat_job_a (torque_drv_t *drv,
            char *command);

static int
stat_queue_a (torque_drv_t *drv,
              char *command);

static ErlDrvTermData *
make_attr_result (torque_drv_t *drv,
                  struct attrl *attr,
                  ErlDrvTermData *result,
                  size_t *result_idx);

static char *
get_attr_name (size_t attr_id);

static struct attrl *
make_attrl_filter (torque_drv_t *drv,
                   char *filter);

static int
send_msg (torque_drv_t *drv,
          const char *tag,
          const char *msg);

static int
send_atom (torque_drv_t *drv,
           char *atom);
