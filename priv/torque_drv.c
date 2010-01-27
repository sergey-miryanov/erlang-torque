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
          fprintf (drv->log, "Or try to connect to fallback server: %s\n", pbs_fbserver);
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
