// Delete dnames from the tokudb.directory.
//
// Requirements:
// The directory containing the tokudb environment is passed as a parameter.
// Needs the log*.tokulog* crash recovery log files.
// Needs a clean shutdown in the recovery log.
// Needs the tokudb.* metadata files.
//
// Effects:
// Deletes dnames from the tokudb.directory.
// Creates a new crash recovery log.

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <assert.h>
#include <db.h>

static int delete_dname(DB_ENV *env, DB_TXN *txn, DB *db, char *dname) {
    DBT delkey = { .data = dname, .size = strlen(dname)+1 };
    int r = db->del(db, txn, &delkey, 0);
    assert(r == 0);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "tokudb_delete_iname DATADIR DNAMES\n");
        return 1;
    }
    char *datadir = argv[1];

    // open the env
    int r;
    DB_ENV *env = NULL;
    r = db_env_create(&env, 0);
    assert(r == 0);

    env->set_errfile(env, stderr);
    r = env->open(env, datadir, DB_INIT_LOCK+DB_INIT_MPOOL+DB_INIT_TXN+DB_INIT_LOG + DB_PRIVATE+DB_CREATE, 
                  S_IRWXU+S_IRWXG+S_IRWXO);
    // open will fail if the recovery log was not cleanly shutdown
    assert(r == 0);

    // use a single txn to cover all of the inames deletes
    DB_TXN *txn = NULL;
    r = env->txn_begin(env, NULL, &txn, 0);
    assert(r == 0);

    DB *db = env->get_db_for_directory(env);
    assert(db != NULL);

    for (int i = 2; i < argc; i++) {
        r = delete_dname(env, txn, db, argv[i]);
        assert(r == 0);
    }

    r = txn->commit(txn, 0);
    assert(r == 0);

    // close the env
    r = env->close(env, 0);
    assert(r == 0);

    return 0;
}
