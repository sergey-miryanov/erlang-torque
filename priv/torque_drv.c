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

  char *default_server = pbs_default ();
  if (!default_server)
    {
      fprintf (drv->log, "Couldn't obtain default server name\n");
      fflush (drv->log);

      default_server = "";
    }

  drv->pbs_connect = torque_connect (drv, default_server); 
  if (drv->pbs_connect <= 0)
    {
      fprintf (drv->log, "Couldn't connect to PBS (Torque) server\n");
    }
  else
    {
      fprintf (drv->log, "Connect to PBS (Torque) server: %s\n", default_server);
    }

  fflush (drv->log);
  return (ErlDrvData) drv;
}

static void
stop (ErlDrvData drv_data)
{
  torque_drv_t *drv = (torque_drv_t *) (drv_data);
  if (pbs_disconnect (drv->pbs_connect))
    {
      fprintf (drv->log, "Couldn't disconnect from PBS (Torque) server: %d\n", pbs_errno);
    }
  else
    {
      fprintf (drv->log, "Disconnect from PBS (Torque) server\n");
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
  switch (command)
    {
    case CMD_STAT_JOB:
      return stat_job (drv, buf);
    default:
      return send_msg (drv, "error", "Unknown command");
    }

  return 0;
}

static void
ready_async (ErlDrvData drv_data,
             ErlDrvThreadData thread_data)
{
}

static int
check_pbs_error (torque_drv_t *drv, const char *server)
{
  switch (pbs_errno)
    {
    case PBSE_BADHOST:
      if ((server == NULL) || (server[0] == '\0'))
        {
          fprintf(drv->log, "Cannot resolve default server host '%s' - check server_name file.\n",
                  pbs_default());
        }
      else
        {
          fprintf(drv->log, "Cannot resolve specified server host '%s'.\n", server);
        }

      break;
    case PBSE_NOCONNECTS:
      fprintf(drv->log, "Too many open connections.\n");
      break;
    case PBSE_NOSERVER:
      fprintf(drv->log, "No default server name - check server_name file.\n");
      break;
    case PBSE_SYSTEM:
      fprintf(drv->log, "System call failure.\n");
      break;
    case PBSE_PERM:
      fprintf(drv->log, "No Permission.\n");
      break;
    case PBSE_PROTOCOL:
    default:
      fprintf(drv->log, "Communication failure.\n");
      break;
    }

  return drv->pbs_connect;
}

static int check_error (torque_drv_t *drv, const char *server)
{
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
    }
  else
    {
      perror(NULL);
    }

  return drv->pbs_connect;
}

static int
torque_connect (torque_drv_t *drv, char *server)
{
  drv->pbs_connect = pbs_connect (server);
  if (drv->pbs_connect <= 0)
    {
      if (pbs_errno > PBSE_)
        {
          return check_pbs_error (drv, server);
        }
      else
        {
          return check_error (drv, server);
        }
    }

  return drv->pbs_connect;
}

static int
stat_job (torque_drv_t *drv,
          char *job_name)
{
  struct batch_status *p_status = pbs_statjob (drv->pbs_connect,
                                               job_name,
                                               NULL,
                                               EXECQUEONLY);
  if (!p_status)
    {
      fprintf (drv->log, "Couldn't obtain job status: (%s, %s)\n",
               job_name, pbse_to_txt (pbs_errno));
      fflush (drv->log);

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
  size_t attr_count = 0;

  result[result_idx++] = ERL_DRV_ATOM;
  result[result_idx++] = driver_mk_atom ("ok");

  struct attrl *attr = p_status->attribs;
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

          pbs_statfree (p_status);
          return send_msg (drv, "error", "Couldn't reallocate memory for driver");
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
  result[result_idx++] = attr_count + 1;
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

