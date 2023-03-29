using System.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using api.Context;
using api.Contracts;
using api.Dto;
using api.Entities;
using Dapper;

namespace api.Repository
{
    public class CompanyRepository : ICompanyRepository
    {
        private readonly DapperContext _context;

        public CompanyRepository(DapperContext context)
        {
            _context = context;
        }

        public async Task<Company> CreateCompany(CompanyForCreationDto company)
        {
            var query = @"
                insert into companies 
                values (@Name, @Address, @Country)
                
                select cast(scope_identity() as int)";

            var parameters = new DynamicParameters();
            parameters.Add("Name", company.Name, DbType.String);
            parameters.Add("Address", company.Address, DbType.String);
            parameters.Add("Country", company.Country, DbType.String);

            using (var connection = _context.CreateConnection())
            {
                var id = await connection.QuerySingleAsync<int>(query, parameters);

                var createdCompany = new Company
                {
                    Id = id,
                    CompanyName = company.Name,
                    Address = company.Address,
                    Country = company.Country,
                };

                return createdCompany;
            }
        }

        public async Task DeleteCompany(int id)
        {
            var query = @"
                delete from companies
                where id = @id";

            using (var connection = _context.CreateConnection())
            {
                await connection.ExecuteAsync(query, new { id });
            }
        }

        public async Task<IEnumerable<Company>> GetCompanies()
        {
            var query = @"
                select 
                    Id, Name as CompanyName, Address, Country 
                from companies";

            using (var connection = _context.CreateConnection())
            {
                var companies = await connection.QueryAsync<Company>(query);
                return companies.ToList();
            }
        }

        public async Task<Company> GetCompany(int id)
        {
            var query = @"
                select 
                    Id, Name as CompanyName, Address, Country 
                from companies
                where Id = @Id";

            using (var connection = _context.CreateConnection())
            {
                var company = await connection
                    .QuerySingleOrDefaultAsync<Company>(query, new { id });
                return company;
            }
        }

        public async Task<Company> GetCompanyByEmployeeId(int id)
        {
            const string procedureName = "ShowCompanyForProvidedEmployeeId";
            var parameters = new DynamicParameters();
            parameters.Add("id", id, DbType.Int32, ParameterDirection.Input);

            using (var connection = _context.CreateConnection())
            {
                var company = await connection
                    .QueryFirstOrDefaultAsync<Company>(
                        procedureName, parameters, commandType: CommandType.StoredProcedure);

                return company;
            }
        }

        public async Task<List<Company>> GetCompanyEmployeesMultipleMapping()
        {
            const string query = @"
                select 
                    companies.Id, 
                    companies.Name as CompanyName, 
                    companies.Address, 
                    companies.Country, 
                    employees.*
                from companies
                join employees on companies.id = employees.companyId";

            using (var connection = _context.CreateConnection())
            {
                var companyDict = new Dictionary<int, Company>();

                var companies = await connection.QueryAsync<Company, Employee, Company>(
                    query, (company, employee) =>
                    {
                        if (!companyDict.TryGetValue(company.Id, out var currentCompany))
                        {
                            currentCompany = company;
                            companyDict.Add(currentCompany.Id, currentCompany);
                        }

                        currentCompany.Employees.Add(employee);
                        return currentCompany;
                    }
                );

                return companies.Distinct().ToList();
            }
        }

        public async Task<Company> GetCompanyEmployeesMultipleResult(int id)
        {
            var query = @"
                select 
                    Id, Name as CompanyName, Address, Country 
                from companies
                where Id = @Id
                
                select 
                    *
                from employees
                where companyId = @Id";

            using (var connection = _context.CreateConnection())
            using (var multi = await connection.QueryMultipleAsync(query, new { id }))
            {
                var company = await multi.ReadSingleOrDefaultAsync<Company>();
                if (company != null)
                {
                    company.Employees = (await multi.ReadAsync<Employee>()).ToList();
                }

                return company;
            }
        }

        public async Task UpdateCompany(int id, CompanyForUpdateDto company)
        {
            var query = @"
                update companies set
                    Name = @Name,
                    Address = @Address,
                    Country = @Country
                where id = @id";

            var parameters = new DynamicParameters();
            parameters.Add("id", id, DbType.Int32);
            parameters.Add("Name", company.Name, DbType.String);
            parameters.Add("Address", company.Address, DbType.String);
            parameters.Add("Country", company.Country, DbType.String);

            using (var connection = _context.CreateConnection())
            {
                await connection.ExecuteAsync(query, parameters);
            }
        }
    }
}