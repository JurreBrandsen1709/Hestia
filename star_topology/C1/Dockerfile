# Use ASP.NET runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

# Copy publish output from local machine
COPY ./star_topology/C1/out .

ENTRYPOINT ["dotnet", "Consumer.dll"]
