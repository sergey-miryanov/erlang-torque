#include "torque_drv.h"
#include <stdio.h>
#include <string.h>

static ErlDrvEntry driver_entry__ = {
  NULL,                             /* init */
  start,                            /* startup (defined below) */
  stop,                             /* shutdown (defined below) */
  NULL,                             /* output */
  NULL,                             /* ready_input */
  NULL,                             /* ready_output */
  "torque_drv",                     /* the name of the driver */
  NULL,                             /* finish */
  NULL,                             /* handle */
  control,                          /* control */
  NULL,                             /* timeout */
  NULL,                             /* outputv (defined below) */
  ready_async,                      /* ready_async */
  NULL,                             /* flush */
  NULL,                             /* call */
  NULL,                             /* event */
  ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
  ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT (drmaa_driver)
{
  return &driver_entry__;
}

static ErlDrvData
start (ErlDrvPort port, char *cmd)
{
  FILE *log = fopen ("/tmp/erlang-torque-drv.log", "a+");
  if (!log)
    {
      fprintf (stderr, "Couldn't create log file\n");
      return 0;
    }

  torque_drv_t *drv = (torque_drv_t *)driver_alloc (sizeof (torque_drv_t));
  if (!drv)
    {
      fprintf (log, "Couldn't allocate memory for driver\n");
      fflush (log);
      fclose (log);
      return 0;
    }
  memset (drv, 0, sizeof (torque_drv_t));

  drv->key  = 46;
  drv->port = port;
  drv->log  = log;
  drv->pbs_connect = 0;

  fflush (drv->log);
  return (ErlDrvData) drv;
}

static void
stop (ErlDrvData drv_data)
{
  torque_drv_t *drv = (torque_drv_t *) (drv_data);

  if (drv->pbs_connect > 0)
    {
      torque_disconnect (drv);
    }

  fclose (drv->log);
  drv->log = 0;

  driver_free (drv);
}

static int
control (ErlDrvData drv_data,
         unsigned int command,
         char *buf,
         int len,
         char **rbuf,
         int rlen)
{
  buf[len] = 0;
  torque_drv_t *drv = (torque_drv_t *) (drv_data);

  if (command == CMD_CONNECT)
    {
      return torque_connect (drv, buf);
    }
  else
    {
      if (drv->pbs_connect <= 0)
        {
          fprintf (drv->log, "Connect to server first\n");
          fflush (drv->log);

          return send_msg (drv, "error", "Connect to server first");
        }

      switch (command)
        {
        case CMD_STAT_JOB:
          return stat_job (drv, buf, NULL);
        case CMD_STAT_QUEUE:
          return stat_queue (drv, buf, NULL);
        case CMD_STAT_JOB_A:
          return stat_job_a (drv, buf);
        case CMD_STAT_QUEUE_A:
          return stat_queue_a (drv, buf);
        default:
          return send_msg (drv, "error", "Unknown command");
        }
    }

  return 0;
}

static void
ready_async (ErlDrvData drv_data,
             ErlDrvThreadData thread_data)
{
}

static const char *
check_pbs_error (torque_drv_t *drv, const char *server)
{
  char *error = "Communication failure";
  switch (pbs_errno)
    {
    case PBSE_BADHOST:
      if ((server == NULL) || (server[0] == '\0'))
        {
          error = "Cannot resolve default server host";
          fprintf(drv->log, "%s '%s' - check server_name file.\n",
                  error, pbs_default());
        }
      else
        {
          error = "Cannot resolve specified server host";
          fprintf(drv->log, "%s '%s'.\n", error, server);
        }

      break;
    case PBSE_NOCONNECTS:
      error = "Too many open connections";
      fprintf(drv->log, "%s.\n", error);
      break;
    case PBSE_NOSERVER:
      error = "No default server name";
      fprintf(drv->log, "%s - check server_name file.\n", error);
      break;
    case PBSE_SYSTEM:
      error = "System call failure";
      fprintf(drv->log, "%s.\n", error);
      break;
    case PBSE_PERM:
      error = "No Permission";
      fprintf(drv->log, "%s.\n", error);
      break;
    case PBSE_PROTOCOL:
    default:
      fprintf(drv->log, "%s.\n", error);
    }

  fflush (drv->log);
  return error;
}

static const char * 
check_error (torque_drv_t *drv, const char *server)
{
  const char *error = "Unknown system error";
  if (errno == ECONNREFUSED)
    {
      if ((server == NULL) || (server[0] == '\0'))
        {
          fprintf(drv->log, "Cannot connect to default server host '%s' - check pbs_server daemon.\n", 
                  pbs_default());
          fprintf (drv->log, "Or try to connect to fallback server: %s\n", pbs_fbserver ());
        }
      else
        {
          fprintf(drv->log, "Cannot connect to specified server host '%s'.\n",
                  server);
        }

      error = "Connection refused";
    }

  fflush (drv->log);
  return error;
}

static int
torque_disconnect (torque_drv_t *drv)
{
  if (pbs_disconnect (drv->pbs_connect))
    {
      fprintf (drv->log, "Couldn't disconnect from PBS (Torque) server: %s\n", pbse_to_txt (pbs_errno));
      fflush (drv->log);
    }
  else
    {
      fprintf (drv->log, "Disconnect from PBS (Torque) server\n");
      fflush (drv->log);
    }

  drv->pbs_connect = 0;
}

static int
torque_connect (torque_drv_t *drv, char *server)
{
  if (drv->pbs_connect > 0)
    {
      fprintf (drv->log, "Driver already connected to server, disconnect and connect to new server: %s\n",
               server);
      fflush (drv->log);

      torque_disconnect (drv);
    }

  if (strlen (server) == 0)
    {
      char *default_server = pbs_default ();
      if (!default_server)
        {
          fprintf (drv->log, "Couldn't obtain default server name\n");
          fflush (drv->log);

          default_server = "";
        }

      server = default_server;
    }

  drv->pbs_connect = pbs_connect (server);
  if (drv->pbs_connect <= 0)
    {
      if (pbs_errno > PBSE_)
        {
          return send_msg (drv, "error", check_pbs_error (drv, server));
        }
      else
        {
          return send_msg (drv, "error", check_error (drv, server));
        }
    }
  else
    {
      fprintf (drv->log, "Connect to PBS (Torque) server: %s\n", server);
      fflush (drv->log);
    }

  return send_atom (drv, "ok");
}

static int
stat_job (torque_drv_t *drv,
          char *job_name,
          struct attrl *filter)
{
  struct batch_status *p_status = pbs_statjob (drv->pbs_connect,
                                               job_name,
                                               filter,
                                               EXECQUEONLY);
  if (!p_status)
    {
      fprintf (drv->log, "Couldn't obtain job status: (%s, %s)\n",
               job_name, pbse_to_txt (pbs_errno));
      fflush (drv->log);

      if (pbs_errno != PBSE_NONE)
        return send_msg (drv, "error", pbse_to_txt (pbs_errno));

      return send_msg (drv, "error", "Couldn't obtain job status");
    }

  ErlDrvTermData *result = (ErlDrvTermData *) driver_alloc (sizeof (ErlDrvTermData) * 2);
  if (!result)
    {
      fprintf (drv->log, "Couldn't allocate memory for driver\n");
      fflush (drv->log);

      pbs_statfree (p_status);
      return send_msg (drv, "error", "Couldn't allocate memory for driver");
    }

  size_t result_idx = 0;
  result[result_idx++] = ERL_DRV_ATOM;
  result[result_idx++] = driver_mk_atom ("ok");

  result = make_attr_result (drv, p_status->attribs, result, &result_idx);
  if (!result)
    {
      pbs_statfree (p_status);
      return 0;
    }

  result = driver_realloc (result, sizeof (*result) * (result_idx + 2));
  if (!result)
    {
      fprintf (drv->log, "Couldn't reallocate memory for driver\n");
      fflush (drv->log);

      pbs_statfree (p_status);
      return send_msg (drv, "error", "Couldn't reallocate memory for driver");
    }

  result[result_idx++] = ERL_DRV_TUPLE;
  result[result_idx++] = 2;

  int r = driver_output_term (drv->port,
                              result,
                              result_idx);

  pbs_statfree (p_status);
  driver_free (result);
  return r;
}

static int
stat_queue (torque_drv_t *drv, 
            char *queue_name,
            struct attrl *filter)
{
  struct batch_status *p_status = pbs_statjob (drv->pbs_connect,
                                               queue_name,
                                               filter,
                                               EXECQUEONLY);
  if (!p_status)
    {
      fprintf (drv->log, "Couldn't obtain queue status: (%s, %s)\n",
               queue_name, pbse_to_txt (pbs_errno));
      fflush (drv->log);

      if (pbs_errno != PBSE_NONE)
        return send_msg (drv, "error", pbse_to_txt (pbs_errno));

      return send_msg (drv, "error", "Couldn't obtain queue status");
    }

  ErlDrvTermData *result = (ErlDrvTermData *) driver_alloc (sizeof (ErlDrvTermData) * 2);
  if (!result)
    {
      fprintf (drv->log, "Couldn't allocate memory for driver\n");
      fflush (drv->log);

      pbs_statfree (p_status);
      return send_msg (drv, "error", "Couldn't allocate memory for driver");
    }

  size_t result_idx = 0;
  result[result_idx++] = ERL_DRV_ATOM;
  result[result_idx++] = driver_mk_atom ("ok");

  struct batch_status *job = p_status;

  size_t job_idx = 0;
  for (; job; ++job_idx, job = job->next)
    {
      result = (ErlDrvTermData *) driver_realloc (result, sizeof (*result) * (result_idx + 3));
      if (!result)
        {
          fprintf (drv->log, "Couldn't reallocate memory for driver\n");
          fflush (drv->log);

          pbs_statfree (p_status);
          return send_msg (drv, "error", "Couldn't reallocate memory for driver");
        }

      result[result_idx++] = ERL_DRV_STRING;
      result[result_idx++] = (ErlDrvTermData)job->name;
      result[result_idx++] = strlen (job->name);

      result = make_attr_result (drv, job->attribs, result, &result_idx);
      if (!result)
        {
          pbs_statfree (p_status);
          return 0;
        }

      result = (ErlDrvTermData *) driver_realloc (result, sizeof (*result) * (result_idx + 2));
      if (!result)
        {
          fprintf (drv->log, "Couldn't reallocate memory for driver\n");
          fflush (drv->log);

          pbs_statfree (p_status);
          return send_msg (drv, "error", "Couldn't reallocate memory for driver");
        }

      result[result_idx++] = ERL_DRV_TUPLE;
      result[result_idx++] = 2;
    }

  result = driver_realloc (result, sizeof (*result) * (result_idx + 5));
  if (!result)
    {
      fprintf (drv->log, "Couldn't reallocate memory for driver\n");
      fflush (drv->log);

      pbs_statfree (p_status);
      return send_msg (drv, "error", "Couldn't reallocate memory for driver");
    }

  result[result_idx++] = ERL_DRV_NIL;
  result[result_idx++] = ERL_DRV_LIST;
  result[result_idx++] = job_idx + 1;
  result[result_idx++] = ERL_DRV_TUPLE;
  result[result_idx++] = 2;

  int r = driver_output_term (drv->port,
                              result,
                              result_idx);

  pbs_statfree (p_status);
  driver_free (result);
  return r;
}

static int
stat_job_a (torque_drv_t *drv,
            char *command)
{
  char *job_name = command;
  command = strstr (command, ",");
  if (!command)
    {
      fprintf (drv->log, "Command should contains not only job name\n");
      fflush (drv->log);

      return stat_job (drv, job_name, NULL);
    }

  command[0] = 0;

  struct attrl *filter = make_attrl_filter (drv, command + 1);
  if (!filter)
    {
      return 0;
    }

  int r = stat_job (drv, job_name, filter);
  driver_free (filter);

  return r;
}

static int
stat_queue_a (torque_drv_t *drv,
              char *command)
{
  char *queue_name = command;
  command = strstr (command, ",");
  if (!command)
    {
      fprintf (drv->log, "Command should contains not only queue name\n");
      fflush (drv->log);

      return stat_queue (drv, queue_name, NULL);
    }

  command[0] = 0;

  struct attrl *filter = make_attrl_filter (drv, command + 1);
  if (!filter)
    {
      return 0;
    }

  int r = stat_queue (drv, queue_name, filter);
  driver_free (filter);

  return r;
}

static struct attrl *
make_attrl_filter (torque_drv_t *drv,
                   char *command)
{
  char *attr_count = command;
  command = strstr (command, ",");
  if (!command)
    {
      fprintf (drv->log, "Invalid command: %s\n", command);
      fflush (drv->log);

      send_msg (drv, "error", "Invalid command");
      return 0;
    }

  ++command;

  struct attrl *attrs = (struct attrl *) driver_alloc (sizeof (struct attrl) * atoi (attr_count));
  if (!attrs)
    {
      fprintf (drv->log, "Couldn't allocate memory for attributes list\n");
      fflush (drv->log);

      send_msg (drv, "error", "Couldn't allocate memory");
      return 0;
    }

  memset (attrs, 0, sizeof (struct attrl) * atoi (attr_count));

  struct attrl *attr = attrs;
  char *attr_id  = command;
  char *attr_str = strstr (command, ",");
  while (attr_id)
    {
      attr->next = attr + 1;
      attr->name = get_attr_name (atoi (attr_id));
      attr->resource = "";
      attr->value = "";
      ++attr;

      if (!attr_str)
        break;

      attr_id = attr_str + 1;
      attr_str = strstr (attr_str + 1, ",");
    }

  attr = attrs;
  while (attr->next)
    {
      fprintf (drv->log, "name: %s\n", attr->name);
      fflush (drv->log);

      attr = attr->next;
    }

  return attrs;
}

static ErlDrvTermData *
make_attr_result (torque_drv_t *drv,
                  struct attrl *attr,
                  ErlDrvTermData *result,
                  size_t *p_result_idx)
{
  size_t result_idx = *p_result_idx;
  size_t attr_count = 0;
  for (;attr; ++attr_count)
    {
      size_t item_count = 2;
      if (attr->resource)
        ++item_count;

      result = driver_realloc (result, sizeof (*result) * (result_idx + item_count * 3 + 3));
      if (!result)
        {
          fprintf (drv->log, "Couldn't reallocate memory for driver\n");
          fflush (drv->log);

          send_msg (drv, "error", "Couldn't reallocate memory for driver");
          return 0;
        }

      result[result_idx++] = ERL_DRV_STRING;
      result[result_idx++] = (ErlDrvTermData)attr->name;
      result[result_idx++] = strlen (attr->name);

      if (attr->resource)
        {
          result[result_idx++] = ERL_DRV_STRING;
          result[result_idx++] = (ErlDrvTermData)attr->resource;
          result[result_idx++] = strlen (attr->resource);
        }

      result[result_idx++] = ERL_DRV_STRING;
      result[result_idx++] = (ErlDrvTermData)attr->value;
      result[result_idx++] = strlen (attr->value);

      result[result_idx++] = ERL_DRV_NIL;
      result[result_idx++] = ERL_DRV_LIST;
      result[result_idx++] = item_count + 1;

      attr = attr->next;
    }

  result = driver_realloc (result, sizeof (*result) * (result_idx + 3));
  if (!result)
    {
      fprintf (drv->log, "Couldn't reallocate memory for driver\n");
      fflush (drv->log);

      send_msg (drv, "error", "Couldn't reallocate memory for driver");
      return 0;
    }

  result[result_idx++] = ERL_DRV_NIL;
  result[result_idx++] = ERL_DRV_LIST;
  result[result_idx++] = attr_count + 1;

  *p_result_idx = result_idx;
  return result;
}

static char *
get_attr_name (size_t attr_id)
{
  switch (attr_id)
    {
    case ATTR_EXECUTION_TIME:   return ATTR_a;
    case ATTR_ACCOUNT_NAME:     return ATTR_A;
    case ATTR_CHECKPOINT:       return ATTR_c;
    case ATTR_ERROR_PATH:       return ATTR_e;
    case ATTR_GROUP_LIST:       return ATTR_g;
    case ATTR_HOLD_TYPES:       return ATTR_h;
    case ATTR_JOIN_PATHS:       return ATTR_j;
    case ATTR_KEEP_FILES:       return ATTR_k;
    case ATTR_RESOURCE_LIST:    return ATTR_l;
    case ATTR_MAIL_POINTS:      return ATTR_m;
    case ATTR_MAIL_USERS:       return ATTR_M;
    case ATTR_JOB_NAME:         return ATTR_N;
    case ATTR_OUTPUT_PATH:      return ATTR_o;
    case ATTR_PRIORITY:         return ATTR_p;
    case ATTR_DESTINATION:      return ATTR_q;
    case ATTR_RERUNABLE:        return ATTR_r;
    case ATTR_SESSION_ID:       return ATTR_session;
    case ATTR_SHELL_PATH_LIST:  return ATTR_S;
    case ATTR_USER_LIST:        return ATTR_u;
    case ATTR_VARIABLE_LIST:    return ATTR_v;
    case ATTR_CREATE_TIME:      return ATTR_ctime;
    case ATTR_DEPEND:           return ATTR_depend;
    case ATTR_MODIF_TIME:       return ATTR_mtime;
    case ATTR_QUEUE_TIME:       return ATTR_qtime;
    case ATTR_QUEUE_TYPE:       return ATTR_qtype;
    case ATTR_STAGEIN:          return ATTR_stagein;
    case ATTR_STAGEOUT:         return ATTR_stageout;
    case ATTR_JOB_STATE:        return ATTR_state;
    }
}

static int
send_msg (torque_drv_t *drv,
          const char *tag,
          const char *msg)
{
  ErlDrvTermData spec [] = {
      ERL_DRV_ATOM, driver_mk_atom ("error"),
      ERL_DRV_STRING, (ErlDrvTermData)msg, strlen (msg),
      ERL_DRV_TUPLE, 2
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}
static int
send_atom (torque_drv_t *drv, char *atom)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom (atom),
      ERL_DRV_TUPLE, 1
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}

