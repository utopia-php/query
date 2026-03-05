<?php

namespace Tests\Query\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Hook\PermissionFilterHook;
use Utopia\Query\Hook\TenantFilterHook;

class FilterHookTest extends TestCase
{
    // ── TenantFilterHook ──

    public function testTenantSingleId(): void
    {
        $hook = new TenantFilterHook(['t1']);
        $condition = $hook->filter('users');

        $this->assertEquals('_tenant IN (?)', $condition->getExpression());
        $this->assertEquals(['t1'], $condition->getBindings());
    }

    public function testTenantMultipleIds(): void
    {
        $hook = new TenantFilterHook(['t1', 't2', 't3']);
        $condition = $hook->filter('users');

        $this->assertEquals('_tenant IN (?, ?, ?)', $condition->getExpression());
        $this->assertEquals(['t1', 't2', 't3'], $condition->getBindings());
    }

    public function testTenantCustomColumn(): void
    {
        $hook = new TenantFilterHook(['t1'], 'organization_id');
        $condition = $hook->filter('users');

        $this->assertEquals('organization_id IN (?)', $condition->getExpression());
        $this->assertEquals(['t1'], $condition->getBindings());
    }

    // ── PermissionFilterHook ──

    public function testPermissionWithRoles(): void
    {
        $hook = new PermissionFilterHook('mydb', ['role:admin', 'role:user']);
        $condition = $hook->filter('documents');

        $this->assertEquals(
            '_uid IN (SELECT DISTINCT _document FROM mydb_documents_perms WHERE _permission IN (?, ?) AND _type = ?)',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'role:user', 'read'], $condition->getBindings());
    }

    public function testPermissionEmptyRoles(): void
    {
        $hook = new PermissionFilterHook('mydb', []);
        $condition = $hook->filter('documents');

        $this->assertEquals('1 = 0', $condition->getExpression());
        $this->assertEquals([], $condition->getBindings());
    }

    public function testPermissionCustomType(): void
    {
        $hook = new PermissionFilterHook('mydb', ['role:admin'], 'write');
        $condition = $hook->filter('documents');

        $this->assertEquals(
            '_uid IN (SELECT DISTINCT _document FROM mydb_documents_perms WHERE _permission IN (?) AND _type = ?)',
            $condition->getExpression()
        );
        $this->assertEquals(['role:admin', 'write'], $condition->getBindings());
    }

    public function testPermissionCustomDocumentColumn(): void
    {
        $hook = new PermissionFilterHook('mydb', ['role:admin'], 'read', '_doc_id');
        $condition = $hook->filter('documents');

        $this->assertStringStartsWith('_doc_id IN', $condition->getExpression());
    }
}
