import {task, productionTask, run, assemblyInfo, msbuild, nunit, nuget, git, buildVariables, localNuget, fsUtils, buildFlags, yarn} from '@avensia/garn';
import * as path from 'path';

const solutionPath = path.join(buildFlags.rootPath, 'log4net.Azure.sln');
const sharedAssemblyInfoPath = path.join(buildFlags.rootPath, 'SharedAssemblyInfo.cs');

const msbuildAndNugetConfig: nuget.RegisterTasksType | msbuild.RegisterTasksType = {
  nugetPath: path.join(buildFlags.rootPath, '.nuget/NuGet.exe'),
  solutionPath,
  nugetConfigPath: path.join(buildFlags.rootPath, 'NuGet.config'),
  projects: [{
    nuGetId: 'Avensia.log4net.Azure',
    projectName: 'log4net.Appender.Azure',
    description: 'Azure Appender for log4net',
    dependencies: [
      'log4net',
      'WindowsAzure.Storage',
    ],
    files: [
      'log4net.Appender.Azure.dll',
    ],
  }],
  authors: 'Avensia AB',
  owners: 'Avensia AB',
  pushToAllInternalServers: true,
};

git.registerTasks({releaseBranch: 'master'});
assemblyInfo.registerTasks({sharedAssemblyInfoPath});
nuget.registerTasks(msbuildAndNugetConfig);
msbuild.registerTasks(msbuildAndNugetConfig);

task('nugetrestore', ['nuget:restore']);

task('clean', ['nugetrestore', 'msbuild:clean'], async () => {
  await fsUtils.rmRf(path.join(buildFlags.rootPath, 'obj'));
  await fsUtils.rmRf(path.join(buildFlags.rootPath, 'bin'));
});

task('tag-version', ['git:tag']);

task('build', ['clean', 'nugetrestore', 'msbuild:rebuild']);

task('package', ['assembly-info:create', 'build', 'msbuild:publish', 'nuget:spec', 'nuget:package']);

task('publish', ['package', 'nuget:push']);

productionTask('continuous-integration', ['build']);

productionTask('continuous-integration-publish', ['publish']);

run();
