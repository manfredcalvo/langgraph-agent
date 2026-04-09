"""
Lakebase Autoscaling - Role Creation & Schema-Level Permissions

Creates a Postgres role for the app's service principal and grants
schema-level permissions. Table creation is handled by the app itself
on startup, so the SP owns all tables.

Prerequisites:
  pip install "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0"

Usage:
  python3 scripts/lakebase-role-setup.py \
    --profile <databricks-cli-profile> \
    --project-id <lakebase-project-id> \
    --sp-client-id <service-principal-client-id>

To find these values:
  - profile: your Databricks CLI profile name
  - project-id: from `databricks postgres list-projects --profile <profile>`
  - sp-client-id: from `databricks apps get <app-name> --profile <profile> --output json`
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Create Lakebase role and grant schema-level permissions for a Databricks App SP"
    )
    parser.add_argument("--profile", required=False, default=None, help="Databricks CLI profile name")
    parser.add_argument("--project-id", required=True, help="Lakebase project ID")
    parser.add_argument("--sp-client-id", required=True, help="App service principal client ID (UUID)")
    args = parser.parse_args()

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.postgres import (
            Role, RoleRoleSpec, RoleMembershipRole,
            RoleIdentityType, RoleAuthMethod
        )
        import psycopg
    except ImportError:
        print('Missing dependencies. Install with:')
        print('  pip install "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0"')
        sys.exit(1)

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    branch = f"projects/{args.project_id}/branches/production"
    endpoint_name = f"{branch}/endpoints/primary"
    sp = args.sp_client_id

    # Step 1: Create Postgres role for the service principal (idempotent)
    print("Step 1: Creating Postgres role for service principal...")
    try:
        w.postgres.create_role(
            parent=branch,
            role_id="app-sp",
            role=Role(
                spec=RoleRoleSpec(
                    postgres_role=sp,
                    identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                    auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                    membership_roles=[RoleMembershipRole.DATABRICKS_SUPERUSER]
                )
            )
        )
        print("  Role created successfully")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  Role already exists, skipping")
        else:
            raise

    # Step 2: Get endpoint host
    print("\nStep 2: Getting Lakebase endpoint host...")
    endpoint = w.postgres.get_endpoint(name=endpoint_name)
    host = endpoint.status.hosts.host
    print(f"  PGHOST: {host}")

    # Step 3: Connect as admin user and grant schema-level permissions
    print("\nStep 3: Connecting to database...")
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    user = w.current_user.me()

    conn = psycopg.connect(
        host=host,
        dbname="databricks_postgres",
        user=user.user_name,
        password=cred.token,
        sslmode="require"
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("\nStep 4: Granting schema-level permissions...")
    grants = [
        (f'GRANT CONNECT ON DATABASE databricks_postgres TO "{sp}"', "CONNECT on database"),
        (f'GRANT CREATE ON DATABASE databricks_postgres TO "{sp}"', "CREATE on database"),
        ("CREATE SCHEMA IF NOT EXISTS public", "Ensure public schema exists"),
        (f'GRANT USAGE ON SCHEMA public TO "{sp}"', "USAGE on public"),
        (f'GRANT CREATE ON SCHEMA public TO "{sp}"', "CREATE on public"),
        (f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{sp}"', "ALL on existing tables"),
        (f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "{sp}"', "Default privileges on future tables"),
    ]

    for sql, desc in grants:
        try:
            cur.execute(sql)
            print(f"  OK: {desc}")
        except Exception as e:
            print(f"  WARN: {desc} -> {e}")

    conn.close()

    print("\n" + "=" * 50)
    print("Done! Schema-level permissions granted.")
    print("The app will create its own tables on startup (SP owns them).")
    print("=" * 50)
    print(f"\nValues for app.yaml:")
    print(f"  PGHOST: {host}")
    print(f"  PGUSER: {sp}")
    print(f"  PGDATABASE: databricks_postgres")
    print(f"  PGPORT: 5432")
    print(f"  PGSSLMODE: require")
    print(f"  LAKEBASE_ENDPOINT_NAME: {endpoint_name}")


if __name__ == "__main__":
    main()
