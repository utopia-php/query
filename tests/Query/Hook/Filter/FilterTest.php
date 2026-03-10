<?php

namespace Tests\Query\Hook\Filter;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Hook\Filter\Permission;
use Utopia\Query\Hook\Filter\Tenant;

class FilterTest extends TestCase
{
    public function testTenantSingleId(): void
    {
        $hook = new Tenant(['t1']);
        $condition = $hook->filter('users');

        $this->assertEquals('tenant_id IN (?)', $condition->getExpression());
        $this->assertEquals(['t1'], $condition->getBindings());
    }

    public function testTenantMultipleIds(): void
    {
        $hook = new Tenant(['t1', 't2', 't3']);
        $condition = $hook->filter('users');

        $this->assertEquals('tenant_id IN (?, ?, ?)', $condition->getExpression());
        $this->assertEquals(['t1', 't2', 't3'], $condition->getBindings());
    }

    public function testTenantCustomColumn(): void
    {
        $hook = new Tenant(['t1'], 'organization_id');
        $condition = $hook->filter('users');

        $this->assertEquals('organization_id IN (?)', $condition->getExpression());
        $this->assertEquals(['t1'], $condition->getBindings());
    }

    public function testPermissionWithRoles(): void
    {
        $hook = new Permission(
            roles: ['role:admin', 'role:user'],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
        );
        $condition = $hook->filter('documents');

        $this->assertEquals(
            'id IN (SELECT DISTINCT document_id FROM mydb_documents_perms WHERE role IN (?, ?) AND type = ?)',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'role:user', 'read'], $condition->getBindings());
    }

    public function testPermissionEmptyRoles(): void
    {
        $hook = new Permission(
            roles: [],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
        );
        $condition = $hook->filter('documents');

        $this->assertEquals('1 = 0', $condition->getExpression());
        $this->assertEquals([], $condition->getBindings());
    }

    public function testPermissionCustomType(): void
    {
        $hook = new Permission(
            roles: ['role:admin'],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
            type: 'write',
        );
        $condition = $hook->filter('documents');

        $this->assertEquals(
            'id IN (SELECT DISTINCT document_id FROM mydb_documents_perms WHERE role IN (?) AND type = ?)',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'write'], $condition->getBindings());
    }

    public function testPermissionCustomDocumentColumn(): void
    {
        $hook = new Permission(
            roles: ['role:admin'],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
            documentColumn: 'doc_id',
        );
        $condition = $hook->filter('documents');

        $this->assertStringStartsWith('doc_id IN', $condition->getExpression());
    }

    public function testPermissionCustomColumns(): void
    {
        $hook = new Permission(
            roles: ['admin'],
            permissionsTable: fn (string $table) => 'acl',
            documentColumn: 'uid',
            permDocumentColumn: 'resource_id',
            permRoleColumn: 'principal',
            permTypeColumn: 'access',
        );
        $condition = $hook->filter('documents');

        $this->assertEquals(
            'uid IN (SELECT DISTINCT resource_id FROM acl WHERE principal IN (?) AND access = ?)',
            $condition->getExpression()
        );
        $this->assertEquals(['admin', 'read'], $condition->getBindings());
    }

    public function testPermissionStaticTable(): void
    {
        $hook = new Permission(
            roles: ['user:123'],
            permissionsTable: fn (string $table) => 'permissions',
        );
        $condition = $hook->filter('any_table');

        $this->assertStringContainsString('FROM permissions', $condition->getExpression());
    }

    public function testPermissionWithColumns(): void
    {
        $hook = new Permission(
            roles: ['role:admin'],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
            columns: ['email', 'phone'],
        );
        $condition = $hook->filter('users');

        $this->assertEquals(
            'id IN (SELECT DISTINCT document_id FROM mydb_users_perms WHERE role IN (?) AND type = ? AND (column IS NULL OR column IN (?, ?)))',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'read', 'email', 'phone'], $condition->getBindings());
    }

    public function testPermissionWithSingleColumn(): void
    {
        $hook = new Permission(
            roles: ['role:user'],
            permissionsTable: fn (string $table) => "{$table}_perms",
            columns: ['salary'],
        );
        $condition = $hook->filter('employees');

        $this->assertEquals(
            'id IN (SELECT DISTINCT document_id FROM employees_perms WHERE role IN (?) AND type = ? AND (column IS NULL OR column IN (?)))',
            $condition->getExpression()
        );
        $this->assertEquals(['role:user', 'read', 'salary'], $condition->getBindings());
    }

    public function testPermissionWithEmptyColumns(): void
    {
        $hook = new Permission(
            roles: ['role:admin'],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
            columns: [],
        );
        $condition = $hook->filter('users');

        $this->assertEquals(
            'id IN (SELECT DISTINCT document_id FROM mydb_users_perms WHERE role IN (?) AND type = ? AND column IS NULL)',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'read'], $condition->getBindings());
    }

    public function testPermissionWithoutColumnsOmitsClause(): void
    {
        $hook = new Permission(
            roles: ['role:admin'],
            permissionsTable: fn (string $table) => "mydb_{$table}_perms",
        );
        $condition = $hook->filter('users');

        $this->assertStringNotContainsString('column', $condition->getExpression());
    }

    public function testPermissionCustomColumnColumn(): void
    {
        $hook = new Permission(
            roles: ['role:admin'],
            permissionsTable: fn (string $table) => 'acl',
            columns: ['email'],
            permColumnColumn: 'field',
        );
        $condition = $hook->filter('users');

        $this->assertEquals(
            'id IN (SELECT DISTINCT document_id FROM acl WHERE role IN (?) AND type = ? AND (field IS NULL OR field IN (?)))',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'read', 'email'], $condition->getBindings());
    }
}
